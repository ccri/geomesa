package org.locationtech.geomesa.filter.function

import java.{lang => jl}

import com.google.gson.stream.JsonReader
import com.jayway.jsonpath.JsonPath
import com.vividsolutions.jts.geom.Geometry
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.{AttributeExpressionImpl, FunctionExpressionImpl, FunctionImpl}
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl.parameter
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.capability.FunctionName

import scala.collection.JavaConversions._


class JsonPath extends FunctionImpl {
  override def getName: String = super.getName

  override def evaluate(`object`: scala.Any): AnyRef = {
    val args = dispatchArguments(`object`)
    val string = args.get("attribute")
    val path  = getParameters.get(1)

    val jsonpath = JsonPath.compile(path.asInstanceOf[AttributeExpressionImpl].getPropertyName, null)

    //val ret = jsonpath.read(string)

    val ret: String = JsonPath.read(string, "$")

    ret
  }

  val ff = CommonFactoryFinder.getFilterFactory2

  //override def getFunctionName: FunctionName = functionName("jsonpath", "result:String", "attribute:String", "jsonpath:String")
  override def getFunctionName: FunctionName =
    ff.functionName("jsonpath",
      Seq(parameter("attribute", classOf[String]), parameter("jsonpath", classOf[String])),
      parameter("return", classOf[String]))
}

//class JsonPath extends FunctionExpressionImpl(
//  new FunctionNameImpl(
//    "jsonpath",
//    classOf[String],
//    parameter("attribute", classOf[String]),
//    parameter("jsonpath", classOf[String])
//  )) {
//
//  def evaluate(feature: SimpleFeature): jl.Object = {
//    println(s"SF Got $feature")
//    "foo"
//  }
//
//  override def evaluate(o: jl.Object): jl.Object = {
//    println(s"Got $o")
//    val string = getExpression(0).evaluate(o)
//    val path = JsonPath.compile(getExpression(1).evaluate(null).asInstanceOf[String], null)
//
//    val ret = path.read(string)
//    ret
//  }
//
//}
