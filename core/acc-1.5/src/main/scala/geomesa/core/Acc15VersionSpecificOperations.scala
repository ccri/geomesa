package geomesa.core

import geomesa.core.data.mapreduce.FeatureIngestMapperWrapper
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, BatchWriterConfig, Instance}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job

/**
 * Created by davidm on 4/29/14.
 */
object Acc15VersionSpecificOperations extends VersionSpecificOperations {

  private[Acc15VersionSpecificOperations] val defaultBatchWriterConfig = new BatchWriterConfig()

  override def addArchiveToClasspath(job: Job, path: Path) = job.addArchiveToClassPath(path)

  override def getConnector(instance: Instance, user: String, password: String) =
    instance.getConnector(user, new PasswordToken(password.getBytes))


  override def createBatchWriter(connector: Connector,
                                 tableName: String,
                                 maxMemory: Long,
                                 maxWriteThreads: Int) = {
    val batchWriterConfig = new BatchWriterConfig().setMaxMemory(maxMemory).setMaxWriteThreads(maxWriteThreads)
    connector.createBatchWriter(tableName, batchWriterConfig)
  }

  override def createBatchWriter(connector: Connector,
                                 tableName: String) = connector.createBatchWriter(tableName,
                                                                                  defaultBatchWriterConfig)

  override def createBatchDeleter(connector: Connector,
                                  tableName: String,
                                  auths: Authorizations,
                                  numQueryThreads: Int) =
    connector.createBatchDeleter(tableName, auths, numQueryThreads, defaultBatchWriterConfig)

  override def getFeatureIngestMapperClass() = classOf[FeatureIngestMapperWrapper.FeatureIngestMapper]
}
