package geomesa.core

import org.apache.accumulo.core.client.{BatchDeleter, Connector, Instance, BatchWriter}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.accumulo.core.security.Authorizations

/**
 * Created by davidm on 4/28/14.
 */
trait VersionSpecificOperations {
  def createBatchWriter(tableName: String): BatchWriter
  def createBatchWriter(tableName: String, maxMemory: Long, maxWriteThreads: Int): BatchWriter
  def createBatchDeleter(tableName: String, auths: Authorizations, numQueryThreads: Int): BatchDeleter
  def getConnector(instance: Instance, user: String, password: String): Connector
  def addArchiveToClasspath(job: Job, path: Path)
  def loadClass[T](string: String): Class[T]
}
