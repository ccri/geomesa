package geomesa.core.util.shell

import collection.JavaConversions._
import org.apache.accumulo.core.util.shell.{Command, Shell}
import org.apache.commons.cli.CommandLine
import geomesa.core.data.AccumuloDataStore

class ListFeaturesCommand extends Command {
  override def numArgs() = 0

  override def description() = "Lists GeoMesa features"

  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    val auths = shellState.getConnector().securityOperations().getUserAuthorizations(shellState.getConnector.whoami())
    val tables = shellState.getConnector.tableOperations().list()
    val features = tables.flatMap { table =>
      val ds = new AccumuloDataStore(shellState.getConnector, table, auths, "")
      ds.getTypeNames.map { t => "%s\t%s".format(table, t) }
    }
    shellState.printLines(features.iterator, true)
    0
  }
}
