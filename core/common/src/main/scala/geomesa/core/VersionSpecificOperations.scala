package geomesa.core

import geomesa.core.data.HLWTKVMapper
import org.apache.accumulo.core.client.{BatchDeleter, Connector, Instance, BatchWriter}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

/**
 * Created by davidm on 4/28/14.
 */
trait VersionSpecificOperations {
  def createBatchWriter(connector: Connector, tableName: String): BatchWriter
  def createBatchWriter(connector: Connector, tableName: String, maxMemory: Long, maxWriteThreads: Int): BatchWriter
  def createBatchDeleter(connector: Connector, tableName: String, auths: Authorizations, numQueryThreads: Int): BatchDeleter
  def getConnector(instance: Instance, user: String, password: String): Connector
  def addArchiveToClasspath(job: Job, path: Path)
  def getFeatureIngestMapperClass(): Class[_ <: HLWTKVMapper]
}
