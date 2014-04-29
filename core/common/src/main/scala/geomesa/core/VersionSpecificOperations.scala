package geomesa.core

import org.apache.accumulo.core.client.{Connector, Instance, BatchWriter}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

/**
 * Created by davidm on 4/28/14.
 */
trait VersionSpecificOperations {
  def createBatchWriter(tableName: String): BatchWriter
  def createBatchWriter(tableName: String, maxMemory: Long, maxWriteThreads: Int): BatchWriter
  def getConnector(instance: Instance, user: String, password: String): Connector
  def addArchiveToClasspath(job: Job, path: Path)
  def loadClass[T](string: String): Class[T]
}
