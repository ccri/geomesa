/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import java.util

import com.vividsolutions.jts.geom._
import org.apache.commons.lang.SerializationUtils
import org.apache.spark.sql.SQLTypes._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, JavaTypeInference}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GenericInternalRow, LeafExpression, Literal, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{AnyDataType, DataType, DataTypes, ObjectType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.geomesa.spark.GeoMesaRelation
import org.opengis.filter.{Filter => GTFilter}
import org.opengis.filter.expression.{Expression => GTExpression}
import org.opengis.filter.spatial.BinarySpatialOperator

import scala.util.Try

object SQLRules {
  // new AST expressions

  case class ByteArrayLiteral(repr: InternalRow, array: Array[Byte]) extends LeafExpression with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = DataTypes.BinaryType
  }

  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryType
  }

  case class PointLiteral(repr: InternalRow, point: Point) extends LeafExpression with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = PointType
  }

  case class LineStringLiteral(repr: InternalRow, lineString: LineString) extends LeafExpression with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = LineStringType
  }

  case class PolygonLiteral(repr: InternalRow, polygon: Polygon) extends LeafExpression with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = PolygonType
  }

  // new optimizations rules
  object STContainsRule extends Rule[LogicalPlan] with PredicateHelper {
    import SQLSpatialFunctions._
    import SQLGeometricConstructorFunctions._

    // JNH: NB: Unused.
    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
      case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
      case ScalaUDF(_, _, Seq(_, GeometryLiteral(_, geom)), _) => Some(geom)
      case _ => None
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case sort @ Sort(_, _, _) => sort    // No-op.  Just realizing what we can do:)
        case filt @ Filter(f, lr@LogicalRelation(gmRel: GeoMesaRelation, _, _)) =>
          // TODO: deal with `or`

          // split up conjunctive predicates and extract the st_contains variable
          val (scalaUDFS, otherFilters) = extractScalaUDFs(f)

          if(scalaUDFS.nonEmpty) {
            // we got an st_contains, extract the geometry and set up the new GeoMesa relation with the appropriate
            // CQL filter

            // TODO: only dealing with one st_contains at the moment
            //            val ScalaUDF(func, _, Seq(GeometryLiteral(_, geom), a), _) = st_contains.head
            val ScalaUDF(func, _, Seq(exprA, exprB), _) = scalaUDFS.head

            val cqlFilter = buildGTFilter(func, exprA, exprB)

            cqlFilter match {
              case Some(filter) =>
                val relation = gmRel.copy(filt = ff.and(gmRel.filt, filter))
                // need to maintain expectedOutputAttributes so identifiers don't change in projections
                val newrel = lr.copy(expectedOutputAttributes = Some(lr.output), relation = relation)
                if(otherFilters.nonEmpty) {
                  // if there are other filters, keep them
                  Filter(otherFilters.reduce(And), newrel)
                } else {
                  // if st_contains was the only filter, just return the new relation
                  newrel
                }
              case None =>
                filt
            }

          } else {
            filt
          }
      }
    }

    private def buildGTFilter(func: AnyRef, exprA: Expression, exprB: Expression): Option[GTFilter] =
      for {
        builder <- funcToFF(func)
        gtExprA <- sparkExprToGTExpr(exprA)
        gtExprB <- sparkExprToGTExpr(exprB)
      } yield {
        builder(gtExprA, gtExprB)
      }

    private def extractScalaUDFs(f: Expression) = {
      splitConjunctivePredicates(f).partition {
        // TODO: Add guard which checks to see if the function can be pushed down
        case ScalaUDF(_, _, _, _) => true
        case _ => false
      }
    }

    def funcToFF(func: AnyRef): Option[(GTExpression, GTExpression) => GTFilter] = {
      func match {
        case ST_Contains => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.contains(expr1, expr2))
        case ST_Crosses => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.crosses(expr1, expr2))
        case ST_Disjoint => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.disjoint(expr1, expr2))
        case ST_Equals => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.equal(expr1, expr2))
        case ST_Intersects => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.intersects(expr1, expr2))
        case ST_Overlaps => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.overlaps(expr1, expr2))
        case ST_Touches => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.touches(expr1, expr2))
        case ST_Within => Some((expr1: GTExpression, expr2: GTExpression) =>
          ff.within(expr1, expr2))
        case _ => None
      }
    }

    def sparkExprToGTExpr(expr: org.apache.spark.sql.catalyst.expressions.Expression): Option[org.opengis.filter.expression.Expression] = {
      expr match {
        case PointLiteral(_, point) =>
          Some(ff.literal(point))
        case LineStringLiteral(_, lineString) =>
          Some(ff.literal(lineString))
        case PolygonLiteral(_, polygon) =>
          Some(ff.literal(polygon))
        case GeometryLiteral(_, geom) =>
          Some(ff.literal(geom))
        case AttributeReference(name, _, _, _) =>
          Some(ff.property(name))
        case _ =>
          log.debug(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
          None
      }
    }
  }

  object FoldConstantGeometryRule extends Rule[LogicalPlan] {
    import SQLGeometricConstructorFunctions._
    import SQLGeometricCastFunctions._
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          // Casts
          case ScalaUDF(ST_CastToPoint, PointType, Seq(Literal(g, GeometryType)),
          Seq(GeometryType)) =>
            val point = ST_CastToPoint(g.asInstanceOf[Geometry])
            PointLiteral(PointUDT.serialize(point), point)

          case ScalaUDF(ST_ByteArray, DataTypes.BinaryType, Seq(Literal(string, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
            val array = ST_ByteArray(string.asInstanceOf[UTF8String].toString)
            ByteArrayLiteral(InternalRow(array), array)

          // Geometric Constructors
          case ScalaUDF(ST_Box2DFromGeoHash, GeometryType, Seq(Literal(hash, DataTypes.StringType),
                        Literal(prec, DataTypes.IntegerType)), Seq(DataTypes.StringType, DataTypes.IntegerType)) =>
            val box2D = ST_Box2DFromGeoHash(hash.asInstanceOf[UTF8String].toString, prec.asInstanceOf[Int])
            GeometryLiteral(GeometryUDT.serialize(box2D), box2D)

          case ScalaUDF(ST_MakeBox2D, GeometryType, Seq(Literal(lowerLeft, PointType), Literal(upperRight, PointType)),
               Seq(PointType, PointType)) =>
            val box2D = ST_MakeBox2D(lowerLeft.asInstanceOf[Point], upperRight.asInstanceOf[Point])
            GeometryLiteral(GeometryUDT.serialize(box2D), box2D)

          case ScalaUDF(ST_GeomFromGeoHash, GeometryType, Seq(Literal(hash, DataTypes.StringType),
          Literal(prec, DataTypes.IntegerType)), Seq(DataTypes.StringType, DataTypes.IntegerType)) =>
            val geom = ST_GeomFromGeoHash(hash.asInstanceOf[UTF8String].toString, prec.asInstanceOf[Int])
            GeometryLiteral(GeometryUDT.serialize(geom), geom)

          case ScalaUDF(ST_GeomFromWKT, GeometryType, Seq(Literal(wkt, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
            val geom = ST_GeomFromWKT(wkt.asInstanceOf[UTF8String].toString)
            GeometryLiteral(GeometryUDT.serialize(geom), geom)

          case ScalaUDF(ST_GeomFromWKB, GeometryType, Seq(Literal(array, DataTypes.BinaryType)),
                        Seq(DataTypes.BinaryType)) =>
            val geom = ST_GeomFromWKB(array.asInstanceOf[Array[Byte]])
            GeometryLiteral(GeometryUDT.serialize(geom), geom)

          case ScalaUDF(ST_Point, PointType,
                        Seq(Literal(x, DataTypes.DoubleType), Literal(y, DataTypes.DoubleType)),
                        Seq(DataTypes.DoubleType, DataTypes.DoubleType)) =>
            val geom = ST_Point(x.asInstanceOf[Double], y.asInstanceOf[Double])
            PointLiteral(GeometryUDT.serialize(geom), geom)

          case ScalaUDF(ST_MakePoint, PointType, Seq(Literal(x, DataTypes.DoubleType),
               Literal(y, DataTypes.DoubleType)), Seq(DataTypes.DoubleType, DataTypes.DoubleType)) =>
            val point = ST_MakePoint(x.asInstanceOf[Double], y.asInstanceOf[Double])
            PointLiteral(PointUDT.serialize(point), point)

          case ScalaUDF(ST_PolygonFromText, PolygonType, Seq(Literal(string, DataTypes.StringType)),
               Seq(DataTypes.StringType)) =>
            val polygon = ST_PolygonFromText(string.asInstanceOf[UTF8String].toString)
            PolygonLiteral(PolygonUDT.serialize(polygon), polygon)
        }
      }
    }
  }

  object ScalaUDFRule extends Rule[LogicalPlan] with LazyLogging {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s@ScalaUDF(func, outputType, inputs, inputTypes) =>
            // TODO: Break down by GeometryType
            val newS: Expression = Try {
              try {
                s.eval(null) match {
                  case row: GenericInternalRow =>
                    val ret = GeometryUDT.deserialize(row)
                    GeometryLiteral(row, ret)
                  case other: Any =>
                    Literal(other)
                }
              } catch {
                 case e: Exception =>
                  e.printStackTrace()
                  throw e
              }
            }.getOrElse(s)
            logger.debug(s"Got $s: evaluated to $newS")

            newS

//          case ScalaUDF(ST_GeomFromWKT, GeometryType, Seq(Literal(wkt, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
//            val geom = ST_GeomFromWKT(wkt.asInstanceOf[UTF8String].toString)
//            GeometryLiteral(GeometryUDT.serialize(geom), geom)
        }
      }
    }
  }

  def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(ScalaUDFRule, STContainsRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }

    Seq.empty[Strategy].foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }
}
