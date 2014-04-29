package geomesa.core.data

import java.io.Serializable
import java.util.{Map => JMap}
import org.apache.accumulo.core.client.Connector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.geotools.data.DataAccessFactory.Param
import scala.collection.JavaConversions._

/**
 * Created by davidm on 4/29/14.
 */
object AccumuloDataStoreFactory {
  implicit class RichParam(val p: Param) {
    def lookupOpt[A](params: JMap[String, Serializable]) =
      Option(p.lookUp(params)).asInstanceOf[Option[A]]
  }

  object params {
    val connParam         = new Param("connector", classOf[Connector], "The Accumulo connector", false)
    val instanceIdParam   = new Param("instanceId", classOf[String], "The Accumulo Instance ID", true)
    val zookeepersParam   = new Param("zookeepers", classOf[String], "Zookeepers", true)
    val userParam         = new Param("user", classOf[String], "Accumulo user", true)
    val passwordParam     = new Param("password", classOf[String], "Password", true)
    val authsParam        = new Param("auths", classOf[String], "Accumulo authorizations", true)
    val tableNameParam    = new Param("tableName", classOf[String], "The Accumulo Table Name", true)
    val idxSchemaParam    = new Param("indexSchemaFormat", classOf[String], "The feature-specific index-schema format", false)
    val mockParam         = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
    val mapreduceParam    = new Param("useMapReduce", classOf[String], "Use MapReduce ingest", false)
    val featureEncParam   = new Param("featureEncoding", classOf[String], "The feature encoding format (text or avro). Default is Avro", false, "avro")
  }

  import params._

  def configureJob(job: Job, params: JMap[String, Serializable]): Job = {
    val conf = job.getConfiguration
    conf.set(ZOOKEEPERS, zookeepersParam.lookUp(params).asInstanceOf[String])
    conf.set(INSTANCE_ID, instanceIdParam.lookUp(params).asInstanceOf[String])
    conf.set(ACCUMULO_USER, userParam.lookUp(params).asInstanceOf[String])
    conf.set(ACCUMULO_PASS, passwordParam.lookUp(params).asInstanceOf[String])
    conf.set(TABLE, tableNameParam.lookUp(params).asInstanceOf[String])
    conf.set(AUTHS, authsParam.lookUp(params).asInstanceOf[String])
    conf.set(FEATURE_ENCODING, featureEncParam.lookUp(params).asInstanceOf[String])
    job
  }

  def getMRAccumuloConnectionParams(conf: Configuration): JMap[String,AnyRef] =
    Map(
         zookeepersParam.key -> conf.get(ZOOKEEPERS),
         instanceIdParam.key -> conf.get(INSTANCE_ID),
         userParam.key -> conf.get(ACCUMULO_USER),
         passwordParam.key -> conf.get(ACCUMULO_PASS),
         tableNameParam.key -> conf.get(TABLE),
         authsParam.key -> conf.get(AUTHS),
         featureEncParam.key -> conf.get(FEATURE_ENCODING),
         "useMapReduce" -> "true")
}
