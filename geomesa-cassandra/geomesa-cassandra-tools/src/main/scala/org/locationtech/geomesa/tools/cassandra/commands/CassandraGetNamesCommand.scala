/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.cassandra.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.cassandra.CassandraConnectionParams
import org.locationtech.geomesa.tools.cassandra.commands.CassandraGetNamesCommand.CassandraGetNamesParameters
import org.locationtech.geomesa.tools.common.commands.GetNamesCommand


class CassandraGetNamesCommand(parent: JCommander)
  extends CommandWithCassandraDataStore(parent)
    with GetNamesCommand
    with LazyLogging {

  override val params = new CassandraGetNamesParameters()

}

object CassandraGetNamesCommand {
  @Parameters(commandDescription = "List GeoMesa feature types for a given catalog")
  class CassandraGetNamesParameters extends CassandraConnectionParams {}
}
