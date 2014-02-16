package geomesa.core.util.shell

import org.apache.accumulo.core.util.shell.{Shell, Command}
import org.apache.commons.cli.{Option => Opt, Options, CommandLine}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.feature.`type`.AttributeTypeImpl
import com.twitter.scalding.{Tool, Args, Job, TextLine}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import com.vividsolutions.jts.geom.{Geometry, Coordinate}
import org.joda.time.format.DateTimeFormat
import geomesa.core.index.{SpatioTemporalIndexEntry, TypeInitializer, SpatioTemporalIndexSchema}
import org.joda.time.DateTime
import cascading.accumulo.AccumuloSource
import java.util.UUID
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.mapred.JobConf
import geomesa.core.data.AccumuloDataStore
import geomesa.core.iterators.SpatioTemporalIntersectingIterator
import org.apache.commons.vfs2.impl.VFSClassLoader
import java.net.{URLEncoder, URLDecoder}
import java.io.File

class IngestFeatureCommand extends Command {

  SpatioTemporalIntersectingIterator.initClassLoader(null)

  val pathOpt   = new Opt("path", true, "HDFS path of file to ingest")
  val latOpt    = new Opt("lat", true, "Name of latitude field")
  val lonOpt    = new Opt("lon", true, "Name of longitude field")
  val dtgOpt    = new Opt("dtg", true, "Name of datetime field")
  val dtgFmtOpt = new Opt("dtgfmt", true, "Format of datetime field")
  val idOpt     = new Opt("idfields", true, "Comma separated list of id fields")

  override def numArgs() = 0

  override def description() = "Ingest a feature from a TSV file stored in HDFS"

  import collection.JavaConversions._
  override def execute(fullCommand: String, cl: CommandLine, shellState: Shell): Int = {
    val conn = shellState.getConnector
    val auths = conn.securityOperations().getUserAuthorizations(conn.whoami()).toString
    val params = Map("connector" -> conn, "tableName" -> shellState.getTableName, "auths" -> auths)
    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    val typeName = ds.getTypeNames.head
    val spec = DataUtilities.encodeType(ds.getSchema(typeName))
    val schema = ds.getIndexSchemaFmt(typeName)

    val path     = cl.getOptionValue(pathOpt.getOpt)
    val latField = cl.getOptionValue(latOpt.getOpt)
    val lonField = cl.getOptionValue(lonOpt.getOpt)
    val dtgField = cl.getOptionValue(dtgOpt.getOpt)
    val dtgFmt   = cl.getOptionValue(dtgFmtOpt.getOpt)
    val idFields = cl.getOptionValue(idOpt.getOpt)

    println(s"Ingesting $path (lat = $latField, lon = $lonField, dtgField = $dtgField, dtgfmt = $dtgFmt")

    val fs = FileSystem.newInstance(new Configuration())
    val ingestPath = new Path(s"/tmp/geomesa/ingest/${conn.whoami()}/${shellState.getTableName}/${UUID.randomUUID().toString.take(5)}")
    fs.mkdirs(ingestPath.getParent)

    val libJars = classOf[Command].getClassLoader.asInstanceOf[VFSClassLoader]
      .getFileObjects
      .map(_.getURL.getFile)
      .map { f => URLDecoder.decode(f, "UTF-8").replace("file:", "").replace("!", "") }
      .map { f => new File(f).getAbsolutePath }
      .mkString(",")

    println(libJars)

    val jobConf = new JobConf

    ToolRunner.run(jobConf, new Tool,
      Array(
        "-libjars",                    libJars,
        classOf[SFTIngest].getCanonicalName,
        "--hdfs",
        "--geomesa.ingest.path",       path,
        "--geomesa.ingest.typename",   typeName,
        "--geomesa.ingest.schema",     URLEncoder.encode(schema, "UTF-8"),
        "--geomesa.ingest.idfeatures", idFields,
        "--geomesa.ingest.sftspec",    URLEncoder.encode(spec, "UTF-8"),
        "--geomesa.ingest.latfield",   latField,
        "--geomesa.ingest.lonfield",   lonField,
        "--geomesa.ingest.dtgfield",   dtgField,
        "--geomesa.ingest.dtgfmt",     dtgFmt,
        "--geomesa.ingest.outpath",    ingestPath.toString,
        "--geomesa.ingest.table",      shellState.getTableName,
        "--geomesa.ingest.instance",   conn.getInstance().getInstanceName
      ))

    val failurePath = new Path(ingestPath, "failure")
    fs.mkdirs(failurePath)
    conn.tableOperations().importDirectory(shellState.getTableName, ingestPath.toString, failurePath.toString, true)

    0
  }

  override def getOptions = {
    val options = new Options
    options.addOption(pathOpt)
    options.addOption(latOpt)
    options.addOption(lonOpt)
    options.addOption(dtgOpt)
    options.addOption(dtgFmtOpt)
    options.addOption(idOpt)
  }
}

class SFTTypeInitializer(typeName: String, typeSpec: String) extends TypeInitializer {
  override def getTypeName = typeName
  override def getTypeSpec = typeSpec
}

class AccumuloFeature(sid: String,
                      geom: Geometry,
                      dt: DateTime,
                      attributesMap: Map[String,Object],
                      typeInitializer: TypeInitializer)
  extends SpatioTemporalIndexEntry(sid, geom, Some(dt), typeInitializer) {

  // use all of the attribute-value pairs passed in, but do not overwrite
  // any existing values
  attributesMap
    .filter { case (name, value) => getAttribute(name) == null }
    .foreach { case (name,value) => setAttribute(name, value) }
}

class SFTIngest(args: Args) extends Job(args) {
  lazy val path     = args("geomesa.ingest.path")
  lazy val typeName = args("geomesa.ingest.typename")
  lazy val schema   = URLDecoder.decode(args("geomesa.ingest.schema"), "UTF-8")
  lazy val idFields = args("geomesa.ingest.idfeatures").split(",")
  lazy val sftSpec  = URLDecoder.decode(args("geomesa.ingest.sftspec"), "UTF-8")
  lazy val latField = args("geomesa.ingest.latfield")
  lazy val lonField = args("geomesa.ingest.lonfield")
  lazy val dtgField = args("geomesa.ingest.dtgfield")
  lazy val dtgFmt   = args("geomesa.ingest.dtgfmt")
  lazy val sft = DataUtilities.createType(typeName, sftSpec)
  lazy val geomFactory = JTSFactoryFinder.getGeometryFactory
  lazy val builder = new SimpleFeatureBuilder(sft)
  lazy val dtFormat = DateTimeFormat.forPattern(dtgFmt)
  lazy val idx = SpatioTemporalIndexSchema(schema, sft)
  lazy val out =
    new AccumuloSource(
      args("geomesa.ingest.instance"),
      args("geomesa.ingest.table"),
      args("geomesa.ingest.outpath"))
  lazy val typeInitializer = new SFTTypeInitializer(typeName, sftSpec)

  import collection.JavaConversions._

  lazy val strippedAttributes = sft.getAttributeDescriptors.reverse.drop(3).reverse

  TextLine(path).flatMap('line -> List('key, 'value)) { line: String =>
    try {
      val attrs = line.toString.split("\t")

      val propMap = strippedAttributes.zip(attrs).map { case (descriptor, s) =>
        descriptor.getLocalName -> descriptor.getType.asInstanceOf[AttributeTypeImpl].parse(s)
      }.toMap

      val id = idFields.map { f => propMap(f) }.mkString("_")
      val lat = propMap(latField).asInstanceOf[String].toDouble
      val lon = propMap(lonField).asInstanceOf[String].toDouble
      val dtg = dtFormat.parseDateTime(propMap(dtgField).asInstanceOf[String])

      val geom = geomFactory.createPoint(new Coordinate(lon, lat))

      val entry = new AccumuloFeature(id, geom, dtg, propMap, typeInitializer)
      Some(idx.encode(entry).head)
    } catch {
      case t: Throwable => None
    }
  }.project('key,'value).groupBy('key) { _.sortBy('value).reducers(64) }.write(out)

}
