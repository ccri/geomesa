package org.locationtech.geomesa.tools.common.commands

import com.typesafe.scalalogging.LazyLogging


trait GetNamesCommand extends CommandWithDataStore with LazyLogging {
  val command = "get-names"
  def execute() = {
    logger.info("Running Get Names")
    ds.getTypeNames.foreach(println)
    ds.dispose()
  }
}
