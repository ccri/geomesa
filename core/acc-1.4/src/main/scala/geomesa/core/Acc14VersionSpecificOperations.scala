package geomesa.core

import org.apache.accumulo.core.client.{Connector, Instance}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.filecache.DistributedCache
import geomesa.core.data.mapreduce.FeatureIngestMapperWrapper

/**
 * Created by davidm on 4/29/14.
 */
object Acc14VersionSpecificOperations extends VersionSpecificOperations {

  val DEFAULT_BATCH_WRITER_MAX_MEMORY = 1000L
  val DEFAULT_BATCH_WRITER_MAX_WRITE_THREADS = 1

  override def addArchiveToClasspath(job: Job, path: Path) = DistributedCache.addArchiveToClassPath(path, job.getConfiguration)

  override def getConnector(instance: Instance, user: String, password: String) =
    instance.getConnector(user, password.getBytes)


  override def createBatchWriter(connector: Connector,
                                 tableName: String,
                                 maxMemory: Long = DEFAULT_BATCH_WRITER_MAX_MEMORY,
                                 maxWriteThreads: Int = DEFAULT_BATCH_WRITER_MAX_WRITE_THREADS) =
    connector.createBatchWriter(tableName, maxMemory, maxMemory, maxWriteThreads)

  override def createBatchWriter(connector: Connector, tableName: String) =
    createBatchWriter(connector,
                      tableName,
                      DEFAULT_BATCH_WRITER_MAX_MEMORY,
                      DEFAULT_BATCH_WRITER_MAX_WRITE_THREADS)


  override def createBatchDeleter(connector: Connector,
                                  tableName: String,
                                  auths: Authorizations,
                                  numQueryThreads: Int) =
    connector.createBatchDeleter(tableName,
                                 auths,
                                 numQueryThreads,
                                 DEFAULT_BATCH_WRITER_MAX_MEMORY,
                                 DEFAULT_BATCH_WRITER_MAX_MEMORY,
                                 DEFAULT_BATCH_WRITER_MAX_WRITE_THREADS)

  override def getFeatureIngestMapperClass() = classOf[FeatureIngestMapperWrapper.FeatureIngestMapper]
}
