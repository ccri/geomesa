package geomesa.core.util.shell

import cascading.accumulo.AccumuloSource
import com.google.common.hash.Hashing
import com.twitter.scalding.{Tool, Args, Job, TextLine}
import com.vividsolutions.jts.geom.{Geometry, Coordinate}
import geomesa.core.data.AccumuloDataStore
import geomesa.core.index.{SpatioTemporalIndexEntry, TypeInitializer, SpatioTemporalIndexSchema}
import geomesa.core.iterators.SpatioTemporalIntersectingIterator
import java.io.File
import java.net.{URLClassLoader, URLEncoder, URLDecoder}
import java.util.UUID
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.util.shell.{Shell, Command}
import org.apache.commons.cli.{Option => Opt, Options, CommandLine}
import org.apache.commons.vfs2.impl.VFSClassLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ToolRunner
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.Hints
import org.geotools.feature.`type`.AttributeTypeImpl
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class IngestFeatureCommand extends Command {

  SpatioTemporalIntersectingIterator.initClassLoader(null)

  val pathOpt   = new Opt("path", true, "HDFS path of file to ingest")
  val latOpt    = new Opt("lat", true, "Name of latitude field")
  val lonOpt    = new Opt("lon", true, "Name of longitude field")
  val dtgOpt    = new Opt("dtg", true, "Name of datetime field")
  val dtgFmtOpt = new Opt("dtgfmt", true, "Format of datetime field")
  val idOpt     = new Opt("idfields", true, "Comma separated list of id fields")
  val csvOpt    = new Opt("csv", false, "Data is in CSV")
  val tsvOpt    = new Opt("tsv", false, "Data is in TSV")

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
    val delim    =
      if(cl.hasOption(csvOpt.getOpt)) ","
      else if(cl.hasOption(tsvOpt.getOpt)) "\t"
      else "\t"

    val fs = FileSystem.newInstance(new Configuration())
    val ingestPath = new Path(s"/tmp/geomesa/ingest/${conn.whoami()}/${shellState.getTableName}/${UUID.randomUUID().toString.take(5)}")
    fs.mkdirs(ingestPath.getParent)

    val libJars = buildLibJars

    runInjestJob(libJars, path, typeName, schema, idFields, spec, latField, lonField, dtgField,
      dtgFmt, ingestPath, shellState, conn, delim)

    bulkIngest(ingestPath, fs, conn, shellState)

    0
  }


  def bulkIngest(ingestPath: Path, fs: FileSystem, conn: Connector, shellState: Shell) {
    val failurePath = new Path(ingestPath, "failure")
    fs.mkdirs(failurePath)
    conn.tableOperations().importDirectory(shellState.getTableName, ingestPath.toString, failurePath.toString, true)
  }

  def runInjestJob(libJars: String, path: String, typeName: String, schema: String, idFields: String, spec: String, latField: String, lonField: String, dtgField: String, dtgFmt: String, ingestPath: Path, shellState: Shell, conn: Connector, delim: String) {
    val jobConf = new JobConf

    ToolRunner.run(jobConf, new Tool,
      Array(
        "-libjars", libJars,
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
        "--geomesa.ingest.instance",   conn.getInstance().getInstanceName,
        "--geomesa.ingest.delim",      delim)
    )
  }

  def buildLibJars: String = {
    val accumuloJars = classOf[Command].getClassLoader.asInstanceOf[URLClassLoader]
      .getURLs
      .filter { _.toString.contains("accumulo") }
      .map(u => classPathUrlToAbsolutePath(u.getFile))

    val geomesaJars = classOf[SpatioTemporalIndexEntry].getClassLoader.asInstanceOf[VFSClassLoader]
      .getFileObjects
      .map(u => classPathUrlToAbsolutePath(u.getURL.getFile))

    (accumuloJars ++ geomesaJars).mkString(",")
  }

  override def getOptions = {
    val options = new Options
    List(pathOpt, latOpt, lonOpt, dtgOpt, dtgFmtOpt, idOpt, csvOpt, tsvOpt).map(options.addOption(_))
    options
  }

  def cleanClassPathURL(url: String): String =
    URLDecoder.decode(url, "UTF-8")
      .replace("file:", "")
      .replace("!", "")

  def classPathUrlToAbsolutePath(url: String) =
    new File(cleanClassPathURL(url)).getAbsolutePath

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
  lazy val idFields = args("geomesa.ingest.idfeatures")
  lazy val sftSpec  = URLDecoder.decode(args("geomesa.ingest.sftspec"), "UTF-8")
  lazy val latField = args("geomesa.ingest.latfield")
  lazy val lonField = args("geomesa.ingest.lonfield")
  lazy val dtgField = args("geomesa.ingest.dtgfield")
  lazy val dtgFmt   = args("geomesa.ingest.dtgfmt")
  lazy val delim    = args("geomesa.ingest.delim")
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
  lazy val dtBuilder =
    strippedAttributes.find(_.getLocalName.equals(dtgField)).map {
      case attr if attr.getType.getBinding.equals(classOf[java.lang.Long]) =>
        (obj: AnyRef) => new DateTime(obj.asInstanceOf[java.lang.Long])

      case attr if attr.getType.getBinding.equals(classOf[java.lang.String]) =>
        (obj: AnyRef) => dtFormat.parseDateTime(obj.asInstanceOf[String])
    }.getOrElse(throw new RuntimeException("Cannot parse date"))

  lazy val idBuilder = idFields match {
    case null =>
      val hashFn = Hashing.goodFastHash(64)
      (props: Map[String, AnyRef]) => {
        val hash = hashFn.newHasher()
        props.values.foreach {
          case l: java.lang.Long    => hash.putLong(l)
          case i: java.lang.Integer => hash.putInt(i)
          case d: java.lang.Double  => hash.putDouble(d)
          case dt: java.util.Date   => hash.putLong(dt.getTime)
          case s: java.lang.String  => hash.putString(s)
          case _                    => // leave out
        }
        new String(hash.hash().asBytes())
      }

    case s: String =>
      val idSplit = idFields.split(",")
      (props: Map[String, AnyRef]) => idSplit.map { f => props(f) }.mkString("_")
  }

  def parseFeature(line: String): List[geomesa.core.index.KeyValuePair] = {
    try {
      val attrs = line.toString.split(delim)

      val propMap = strippedAttributes.zip(attrs).map {
        case (descriptor, s) =>
          descriptor.getLocalName -> descriptor.getType.asInstanceOf[AttributeTypeImpl].parse(s)
      }.toMap

      val id = idBuilder(propMap)
      val lat = propMap(latField).asInstanceOf[Double]
      val lon = propMap(lonField).asInstanceOf[Double]
      val dtg = dtBuilder(propMap(dtgField))

      val geom = geomFactory.createPoint(new Coordinate(lon, lat))

      val entry = new AccumuloFeature(id, geom, dtg, propMap, typeInitializer)
      entry.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      idx.encode(entry).toList
    } catch {
      case t: Throwable => List()
    }
  }

  TextLine(path)
    .flatMap('line -> List('key, 'value)) { line: String => parseFeature(line) }
    .project('key,'value)
    .groupBy('key) { _.sortBy('value).reducers(64) }
    .write(out)

}
