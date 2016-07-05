package org.locationtech.geomesa.accumulo.data.stats.usage

import java.util.UUID

import org.apache.metamodel.schema.{ColumnType, Schema, Table}
import org.apache.metamodel.{UpdateCallback, UpdateScript, UpdateableDataContext}
/**
  *
  *
  * @param context metamodel data context
  * @param tableName table to write to, in the default schema.
  */
class QueryStatListener(context: UpdateableDataContext, tableName: String) {

  import QueryStatListener._

  val table = {
    val schema = context.getDefaultSchema
    val t = schema.getTableByName(tableName)
    if (t == null) {
      context.executeUpdate(createTableScript(schema, tableName))
      schema.getTableByName(tableName)
    } else {
      t
    }
  }

  def requestCompleted(queryStatData: QueryStat): Unit =
    context.executeUpdate(createAuditScript(table, queryStatData))

  def requestPostProcessed(queryStatData: QueryStat): Unit = {}
}

object QueryStatListener {

  case class Column(name: String, binding: ColumnType)

  object Columns {
    val Id =                  Column("id",                  ColumnType.STRING)
    val TypeName =            Column("typeName",            ColumnType.STRING)
    val User =                Column("user",                ColumnType.STRING)
    val Date =                Column("date",                ColumnType.DATE)
    val PlanTime =            Column("planTime",            ColumnType.INTEGER)
    val ScanTime =            Column("scanTime",            ColumnType.INTEGER)
    val TotalTime =           Column("totalTime",           ColumnType.INTEGER)
    val Filter =              Column("filter",              ColumnType.STRING)
    val Hints =               Column("hints",               ColumnType.STRING)
    val Hits  =               Column("hits",                ColumnType.INTEGER)
  }

  def createTableScript(schema: Schema, table: String): UpdateScript =
    new UpdateScript {
      import Columns._
      override def run(callback: UpdateCallback): Unit = {
        val update = callback.createTable(schema, table)
        update.withColumn(Id.name).ofType(Id.binding).asPrimaryKey()
        update.withColumn(TypeName.name).ofType(TypeName.binding)
        update.withColumn(User.name).ofType(User.binding)
        update.withColumn(Date.name).ofType(Date.binding)
        update.withColumn(PlanTime.name).ofType(PlanTime.binding)
        update.withColumn(ScanTime.name).ofType(ScanTime.binding)
        update.withColumn(TotalTime.name).ofType(TotalTime.binding)
        update.withColumn(Filter.name).ofType(Filter.binding)
        update.withColumn(Hints.name).ofType(Hints.binding)
        update.withColumn(Hits.name).ofType(Hits.binding)
        update.execute()
      }
    }

  def createAuditScript(table: Table, data: QueryStat): UpdateScript =
    new UpdateScript {
      import Columns._
      override def run(callback: UpdateCallback): Unit = {
        val update = callback.insertInto(table)
        update.value(Id.name, UUID.randomUUID().toString)

        if (data.typeName != null) { update.value(TypeName.name, data.typeName) }
        if (data.user != null) { update.value(User.name, data.user) }
        if (data.filter != null) { update.value(Filter.name, data.filter) }
        update.value(PlanTime.name, data.planTime)
        update.value(ScanTime.name, data.scanTime)
        update.value(TotalTime.name, data.planTime + data.scanTime)
        update.value(Date.name, data.date)
        if (data.hints != null) { update.value(Hints.name, data.hints)}
        update.value(Hits.name, data.hits)
        update.execute()
      }
    }

}
