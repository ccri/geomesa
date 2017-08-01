/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Coordinate, Geometry, Point, Polygon}
import org.apache.spark._
import org.apache.spark.rdd.{CartesianPartition, CartesianRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.unsafe.sort.{RecordComparator, UnsafeExternalSorter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/* A more efficient CartesianRDD that only constructs
 * CartesianPartitions for matching indexes in the two RDDs
 * i.e. 0 -> 0, 1 -> 1, 2 -> 2 as opposed to a cross-product.
 */
class TargetedCartesianRDD(left : RDD[UnsafeRow], right : RDD[UnsafeRow], leftSchema: StructType, rightSchema: StructType)
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  val numPartitions: Int = rdd1.partitions.length
  override val numPartitionsInRdd2: Int = rdd2.partitions.length

  // Instead of doing a cross product here, we know a one-to-one mapping will suffice
  override def getPartitions: Array[Partition] = {
    val array = Array.ofDim[Partition](numPartitions)
    for (index <- 0 until numPartitions) {
      array(index) = new CartesianPartition(index, rdd1, rdd2, index, index)
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = Nil

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {

    import org.apache.spark.sql.execution.SortPrefixUtils

    val geometryOrdinal = rightSchema.indexWhere(_.dataType == SQLTypes.PointTypeInstance)
    val rowOrdering = JoinHelperUtils.rowOrdering(geometryOrdinal, SQLTypes.PointTypeInstance)

    val leftRecordComparator = new SweeplineComparator(rowOrdering, leftSchema.length)
    val rightRecordComparator = new SweeplineComparator(rowOrdering, rightSchema.length)
    val leftPrefixComparator = SortPrefixUtils.getPrefixComparator(rightSchema)
    val rightPrefixComparator = SortPrefixUtils.getPrefixComparator(rightSchema)

    // Create sorters for left and right partitions
    val rightSorter = UnsafeExternalSorter.create(
      context.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      context,
      leftRecordComparator,
      leftPrefixComparator,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      false)

    val leftSorter = UnsafeExternalSorter.create(
      context.taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      context,
      rightRecordComparator,
      rightPrefixComparator,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
        UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD),
      false)

    // Insert records into them
    val partition = split.asInstanceOf[CartesianPartition]
    for (y <- rdd2.iterator(partition.s2, context)) {
      rightSorter.insertRecord(y.getBaseObject, y.getBaseOffset, y.getSizeInBytes, 0)
    }
    for (x <- rdd1.iterator(partition.s1, context)) {
      leftSorter.insertRecord(x.getBaseObject, x.getBaseOffset, x.getSizeInBytes, 0)
    }

    // Create an iterator to pull out the smaller of the two
    val iter = new SweeplineIterator(leftSchema, rightSchema, rowOrdering, geometryOrdinal)
    val sortedGeomSource = iter.createIter(leftSorter, rightSorter)

    // Iterate through it to build minimal potential join list
    var leftList = ListBuffer[UnsafeRow]()
    var joinList = ListBuffer[(UnsafeRow, UnsafeRow)]()
    val leftStack = mutable.Stack[UnsafeRow]()

    while(sortedGeomSource.hasNext) {
      sortedGeomSource.next() match {
        case (0, newRow: UnsafeRow) =>
          // TODO: maintain order and only compare against last
          leftList = leftList.filter { oldRow => rowOrdering.compare(oldRow, newRow) <= 0 }
          leftList = leftList :+ newRow
        case (1, row: UnsafeRow) =>
          for (leftRow <- leftList) {
            joinList = joinList. :+ (leftRow, row)
          }
      }
    }
    leftSorter.cleanupResources()
    rightSorter.cleanupResources()
    joinList.toIterator

  }
}

/* A special catalyst tree node that uses a
 * TargetedCartesianRDD to compute the join.
 * This can be done when the two rdds are spatially
 * partitioned by the same partitioning scheme
 */
case class PartitionedJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode {

  override def output: Seq[Attribute] = left.output ++ right.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
    leftResults.persist(StorageLevel.MEMORY_ONLY)
    val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]
    rightResults.persist(StorageLevel.MEMORY_ONLY)

    val pair = new TargetedCartesianRDD(leftResults, rightResults, left.schema, right.schema)


    println(s"pair count ${pair.count()}")

    // For Spark 2.0.0
    pair.mapPartitionsInternal { iter =>
      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
      val filtered = if (condition.isDefined) {
        val boundCondition: (InternalRow) => Boolean =
          newPredicate(condition.get, left.output ++ right.output)
        val joined = new JoinedRow

        iter.filter { r =>
          boundCondition(joined(r._1, r._2))
        }
      } else {
        iter
      }
      if (filtered.isEmpty) {
        println("empty iter")
      } else {
        println("nonempty iter")
      }
      filtered.map { r =>
        numOutputRows += 1
        joiner.join(r._1, r._2)
      }
    }

    // For Spark 2.1.1
//    pair.mapPartitionsWithIndexInternal { (index, iter) =>
//      println(s"filtering partition $index")
//      val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
//      val filtered = if (condition.isDefined) {
//        var i  = 0
//        val boundCondition = newPredicate(condition.get, left.output ++ right.output)
//        boundCondition.initialize(index)
//        val joined = new JoinedRow
//        iter.filter { r =>
//          if (i % 10000000 == 0) {
//            println(s"partition $index filtered ${i/1000000} million rows at ${System.currentTimeMillis()}")
//          }
//          i+=1
//          boundCondition.eval(joined(r._1, r._2))
//        }
//      } else {
//        iter
//      }
//      if (filtered.isEmpty) {
//        println("empty iter")
//      } else {
//        println("nonempty iter")
//      }
//      filtered.map { r =>
//        numOutputRows += 1
//        joiner.join(r._1, r._2)
//      }
//    }
  }
}

class SweeplineComparator(rowOrdering: Ordering[UnsafeRow], numFields: Int) extends RecordComparator {

  private val row1: UnsafeRow = new UnsafeRow(numFields)
  private val row2: UnsafeRow = new UnsafeRow(numFields)

  override def compare(baseObj1: Any, baseOff1: Long, baseObj2: Any, baseOff2: Long): Int = {
    row1.pointTo(baseObj1, baseOff1, 0)
    row2.pointTo(baseObj2, baseOff2, 0)
    rowOrdering.compare(row1, row2)
  }

}

class SweeplineIterator(leftSchema: StructType, rightSchema: StructType,
                        RowOrdering: Ordering[UnsafeRow], geometryOrdinal: Int) {

  val leftUnsafeRow = new UnsafeRow(leftSchema.length)
  val rightUnsafeRow = new UnsafeRow(rightSchema.length)
  var leftExhausted = false
  var rightExhausted = false

  def createIter(leftSorter: UnsafeExternalSorter, rightSorter: UnsafeExternalSorter): Iterator[(Int, UnsafeRow)] = {
    val leftIter = leftSorter.getIterator
    val rightIter = rightSorter.getIterator
    leftIter.loadNext()
    rightIter.loadNext()
    leftUnsafeRow.pointTo(leftIter.getBaseObject, leftIter.getBaseOffset, leftIter.getRecordLength)
    rightUnsafeRow.pointTo(rightIter.getBaseObject, rightIter.getBaseOffset, rightIter.getRecordLength)

    var leftGeom = JoinHelperUtils.unsafeRowToGeom(geometryOrdinal, SQLTypes.PointTypeInstance, leftUnsafeRow)
    var rightGeom = JoinHelperUtils.unsafeRowToGeom(geometryOrdinal, SQLTypes.PointTypeInstance, rightUnsafeRow)

    new Iterator[(Int, UnsafeRow)] {
      def advanceAndReturnLeft(): (Int, UnsafeRow) = {
        leftUnsafeRow.pointTo(leftIter.getBaseObject, leftIter.getBaseOffset, leftIter.getRecordLength)
        leftGeom = JoinHelperUtils.unsafeRowToGeom(geometryOrdinal, SQLTypes.PointTypeInstance, leftUnsafeRow)
        if (!leftIter.hasNext) {
          leftExhausted = true
        }
        leftIter.loadNext() // Loading next does not modify the current row to be returned.
        (0, leftUnsafeRow)
      }

      def advanceAndReturnRight(): (Int, UnsafeRow) = {
        rightUnsafeRow.pointTo(rightIter.getBaseObject, rightIter.getBaseOffset, rightIter.getRecordLength)
        rightGeom = JoinHelperUtils.unsafeRowToGeom(geometryOrdinal, SQLTypes.PointTypeInstance, rightUnsafeRow)
        if (!rightIter.hasNext) {
          rightExhausted = true
        }
        rightIter.loadNext() // Loading next does not modify the current row to be returned.
        (1, rightUnsafeRow)
      }

      override def hasNext: Boolean = {
        leftIter.hasNext || rightIter.hasNext
      }

      override def next(): (Int, UnsafeRow) = {
        if (leftExhausted) {
          advanceAndReturnRight()
        } else if (rightExhausted) {
          advanceAndReturnLeft()
        } else {
          if (RowOrdering.compare(leftUnsafeRow, rightUnsafeRow) ==  0) {
            advanceAndReturnLeft()
          } else if (leftIter.hasNext && RowOrdering.compare(leftUnsafeRow, rightUnsafeRow) > 0) {
            advanceAndReturnLeft()
          } else {
            advanceAndReturnRight()
          }
        }
      }
    }
  }
}