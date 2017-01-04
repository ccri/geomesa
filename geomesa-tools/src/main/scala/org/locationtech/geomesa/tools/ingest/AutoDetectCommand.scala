/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.ingest

import java.io.{File, FileInputStream, FileReader, InputStream}
import java.util.concurrent.atomic.AtomicLong

import com.beust.jcommander.{Parameter, ParameterException}
import com.sun.deploy.util.SyncFileAccess.FileInputStreamLock
import org.geotools.data.{DataStore, DataStoreFinder}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.tools.{CatalogParam, DataStoreCommand, OptionalInputFormatParam, OptionalTypeNameParam}

trait AutoDetectCommand[DS <: DataStore] extends DataStoreCommand[DS] {

  import scala.collection.JavaConversions._

  override val name = "auto-detect"
  override def params: AutoDetectParams

  def libjarsFile: String
  def libjarsPaths: Iterator[() => Seq[File]]

  override def execute(): Unit = {

    import org.locationtech.geomesa.tools.utils.DataFormats.{Avro, Csv, Tsv}

    ensureSameFs(AutoDetectCommand.RemotePrefixes)

    val converterFile = new File(s"~/${params.featureName}_converter.txt")
    val converter = new AutoIngest(params.featureName, connection, params.files, params.fmt,
      libjarsFile, libjarsPaths, params.threads)
      .createLocalConverter(converterFile, new AtomicLong())
    val ds = DataStoreFinder.getDataStore(connection)

    val sftString = converter.convert(new FileInputStream(params.files.head))._1.toString
    println(sftString)
  }

  def ensureSameFs(prefixes: Seq[String]): Unit = {
    prefixes.foreach { pre =>
      if (params.files.exists(_.toLowerCase.startsWith(s"$pre://")) &&
        !params.files.forall(_.toLowerCase.startsWith(s"$pre://"))) {
        throw new ParameterException(s"Files must all be on the same file system: ($pre) or all be local")
      }
    }
  }
}

trait AutoDetectParams extends  OptionalTypeNameParam with OptionalInputFormatParam {
  @Parameter(names = Array("-t", "--threads"), description = "Number of threads if using local ingest")
  var threads: Integer = 1
}

object AutoDetectCommand {
  // If you change this, update the regex in GeneralShapefileIngest for URLs
  private val RemotePrefixes = Seq("hdfs", "s3n", "s3a")

  def isDistributedUrl(url: String): Boolean = RemotePrefixes.exists(url.startsWith)
}
