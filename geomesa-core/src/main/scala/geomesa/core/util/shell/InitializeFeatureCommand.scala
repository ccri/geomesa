package geomesa.core.util.shell

import org.apache.accumulo.core.util.shell.{Shell, Command}
import org.apache.commons.cli.{Option => Opt, Options, CommandLine}
import org.geotools.data.{DataUtilities, DataStoreFinder}
import geomesa.core.iterators.SpatioTemporalIntersectingIterator

class InitializeFeatureCommand extends Command {
  val schemaOpt = new Opt("is", "indexschema", true, "Custom index schema")

  override def numArgs() = 3

  override def description() = "Create a new GeoMesa feature table with the specified feature type"

  import collection.JavaConversions._
  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    SpatioTemporalIntersectingIterator.initClassLoader(null)

    val conn = shellState.getConnector
    val auths = conn.securityOperations().getUserAuthorizations(conn.whoami()).toString
    val args = cl.getArgs
    val tableName = args(0)
    val featureName = args(1)
    val sftSpec = args(2)

    val params = Map("connector" -> conn, "tableName" -> tableName, "auths" -> auths)
    val finalParams =
      if(cl.hasOption(schemaOpt.getOpt)) params + ("indexSchemaFormat" -> cl.getOptionValue(schemaOpt.getOpt))
      else params

    val sft = DataUtilities.createType(featureName, sftSpec)
    val ds = DataStoreFinder.getDataStore(finalParams)
    ds.createSchema(sft)

    0
  }

  override def getOptions: Options = {
    val options = super.getOptions
    options.addOption(schemaOpt)
    options
  }
}
