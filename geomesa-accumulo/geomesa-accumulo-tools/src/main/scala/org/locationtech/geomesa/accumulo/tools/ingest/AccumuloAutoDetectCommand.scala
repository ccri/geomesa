package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File

import com.beust.jcommander.Parameters
import org.apache.accumulo.core.client.Connector
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.ingest.{AutoDetectCommand, AutoDetectParams}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

/**
  * Created by ckelly on 1/6/17.
  */
class AccumuloAutoDetectCommand extends AutoDetectCommand[AccumuloDataStore] with AccumuloDataStoreCommand {

  override val params = new AccumuloAutoDetectParams()

  override val libjarsFile: String = "org/locationtech/geomesa/accumulo/tools/ingest-libjars.list"

  override def libjarsPaths: Iterator[() => Seq[File]] = Iterator(
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME"),
    () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
    () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
    () => ClassPathUtils.getJarsFromClasspath(classOf[Connector])
  )

}

@Parameters(commandDescription = "Automatically detect what kind of sft and converter is in the file")
class AccumuloAutoDetectParams extends AutoDetectParams with AccumuloDataStoreParams
