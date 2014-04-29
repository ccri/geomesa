package geomesa.core

import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.client.Instance
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path

/**
 * Created by davidm on 4/29/14.
 */
class Acc15VersionSpecificOperations extends VersionSpecificOperations {
  override def loadClass[T](string: String) = ???

  override def addArchiveToClasspath(job: Job, path: Path) = ???

  override def getConnector(instance: Instance, user: String, password: String) = ???

  override def createBatchDeleter(tableName: String,
                                  auths: Authorizations,
                                  numQueryThreads: Int) = ???

  override def createBatchWriter(tableName: String, maxMemory: Long, maxWriteThreads: Int) = ???

  override def createBatchWriter(tableName: String) = ???
}
