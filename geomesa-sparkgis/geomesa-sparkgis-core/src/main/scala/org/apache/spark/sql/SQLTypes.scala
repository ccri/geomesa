package org.apache.spark.sql

import java.lang.Double

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.{JTS, JTSFactoryFinder}
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.sparkgis.GeoMesaRelation
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.opengis.filter.expression.{Expression => GTExpression}
import org.slf4j.LoggerFactory

class GeoMesaSQL

object SQLTypes {

  @transient val log = LoggerFactory.getLogger(classOf[GeoMesaSQL])
  @transient val geomFactory = JTSFactoryFinder.getGeometryFactory
  @transient val ff = CommonFactoryFinder.getFilterFactory2

  val PointType        = new PointUDT
  val LineStringType   = new LineStringUDT
  val PolygonType      = new PolygonUDT
  val MultipolygonType = new MultiPolygonUDT
  val GeometryType     = new GeometryUDT

  UDTRegistration.register(classOf[Point].getCanonicalName, classOf[PointUDT].getCanonicalName)
  UDTRegistration.register(classOf[LineString].getCanonicalName, classOf[LineStringUDT].getCanonicalName)
  UDTRegistration.register(classOf[Polygon].getCanonicalName, classOf[PolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[MultiPolygon].getCanonicalName, classOf[MultiPolygonUDT].getCanonicalName)
  UDTRegistration.register(classOf[Geometry].getCanonicalName, classOf[GeometryUDT].getCanonicalName)

  // Spatial Predicates
  //val ST_Contains: (Geometry, Geometry) => Boolean = (p, geom) => geom.contains(p)
  // JNH: Not sure about this one
  //val ST_ContainsProperly: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom2.contains(geom1) && !geom1.intersects(geom2.getBoundary)

  val ST_Contains: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.contains(geom2)
  //val ST_ContainsProperly: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.containsproperly(geom2)
  val ST_Covers: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.covers(geom2)
  //val ST_CoveredBy: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.coveredby(geom2)
  val ST_Crosses: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.crosses(geom2)
  val ST_Disjoint: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.disjoint(geom2)
  val ST_Equals: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.equals(geom2)
  val ST_Intersects: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.intersects(geom2)
  val ST_Overlaps: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.overlaps(geom2)
  val ST_Touches: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.touches(geom2)
  val ST_Within: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.within(geom2)


  // geometry constructors
  val ST_MakeBox2D: (Point, Point) => Polygon = (ll, ur) => JTS.toGeometry(new Envelope(ll.getX, ur.getX, ll.getY, ur.getY))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Polygon = (lx, ly, ux, uy) => JTS.toGeometry(new Envelope(lx, ux, ly, uy))

  // geometry functions
  val ST_Envelope:  Geometry => Geometry = p => p.getEnvelope
  val ST_Centroid: Geometry => Point = g => g.getCentroid

  @transient private val geoCalcs = new ThreadLocal[GeodeticCalculator] {
    override def initialValue(): GeodeticCalculator = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  }

  def fastDistance(s: Geometry, e: Geometry): Double = {
    val calc = geoCalcs.get()
    val c1 = s.getCentroid.getCoordinate
    calc.setStartingGeographicPoint(c1.x, c1.y)
    val c2 = e.getCentroid.getCoordinate
    calc.setDestinationGeographicPoint(c2.x, c2.y)
    calc.getOrthodromicDistance
  }

  // TODO: Make this work for geometry
  val ST_DistanceSpheroid: (Geometry, Geometry) => java.lang.Double = (s, e) => fastDistance(s, e)

  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]

  val ch = new ConvexHull

  // TODO: optimize when used as a literal
  // e.g. select * from feature where st_contains(geom, geomFromText('POLYGON((....))'))
  // should not deserialize the POLYGON for every call
  val ST_GeomFromWKT: String => Geometry = s => WKTUtils.read(s)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register spatial predicates.
//    sqlContext.udf.register("st_contains"      , ST_Contains)
//    sqlContext.udf.register("st_within"        , ST_Contains) // TODO: is contains different than within?

    sqlContext.udf.register("st_contains"      , ST_Contains)
//    sqlContext.udf.register("st_containsproperly"      , ST_ContainsProperly)
    sqlContext.udf.register("st_covers"      , ST_Covers)
//    sqlContext.udf.register("st_coveredby"      , ST_CoveredBy)
    sqlContext.udf.register("st_crosses"      , ST_Crosses)
    sqlContext.udf.register("st_disjoint"      , ST_Disjoint)
    sqlContext.udf.register("st_equals"      , ST_Equals)
    sqlContext.udf.register("st_intersects"      , ST_Intersects)
    sqlContext.udf.register("st_overlaps"      , ST_Overlaps)
    sqlContext.udf.register("st_touches"      , ST_Touches)
    sqlContext.udf.register("st_within"      , ST_Within)

    sqlContext.udf.register("st_geomFromWKT"   , ST_GeomFromWKT)

    sqlContext.udf.register("st_envelope"      , ST_Envelope)
    sqlContext.udf.register("st_makeBox2D"     , ST_MakeBox2D)
    sqlContext.udf.register("st_makeBBOX"      , ST_MakeBBOX)
    sqlContext.udf.register("st_centroid"      , ST_Centroid)
    sqlContext.udf.register("st_castToPoint"   , ST_CastToPoint)

    sqlContext.udf.register("st_distanceSpheroid"  , ST_DistanceSpheroid)

    sqlContext.udf.register("st_convexhull", ch)


    // JNH: The next two lines demonstrate adding ScalaUDFs directly.
//    def containsBuilder(e: Seq[Expression]) = ScalaUDF(ST_Contains, BooleanType, e, Seq(GeometryType, GeometryType))
//    sqlContext.sparkSession.sessionState.functionRegistry.registerFunction("st_contains", containsBuilder)
  }

  class ConvexHull extends UserDefinedAggregateFunction {
    val geomtryType = DataTypes.createStructField("inputGeometry", SQLTypes.GeometryType, true)

    override def inputSchema: StructType = DataTypes.createStructType(Array(geomtryType))

    override def bufferSchema: StructType = DataTypes.createStructType(Array(geomtryType))

    override def dataType: DataType = DataTypes.createStructType(Array(geomtryType))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, null)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println(s"In update with $buffer and $input")
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      println(s"In merge with $buffer1 and $buffer2")
    }

    override def evaluate(buffer: Row): Any = {
      println(s"In evaluate with $buffer")

    }
  }


  // new AST expressions
  case class GeometryLiteral(repr: InternalRow, geom: Geometry) extends LeafExpression  with CodegenFallback {

    override def foldable: Boolean = true

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any = repr

    override def dataType: DataType = GeometryType

  }

  // new optimizations rules
 object STContainsRule extends Rule[LogicalPlan] with PredicateHelper {

    // JNH: NB: Unused.
    def extractGeometry(e: org.apache.spark.sql.catalyst.expressions.Expression): Option[Geometry] = e match {
       case And(l, r) => extractGeometry(l).orElse(extractGeometry(r))
       case ScalaUDF(ST_Contains, _, Seq(_, GeometryLiteral(_, geom)), _) => Some(geom)
       case _ => None  
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      println("HERE!")
      plan.transform {
        case sort @ Sort(_, _, _) => sort    // No-op.  Just realizing what we can do:)
        case filt @ Filter(f, lr@LogicalRelation(gmRel: GeoMesaRelation, _, _)) =>
          // TODO: deal with `or`

          // split up conjunctive predicates and extract the st_contains variable
          val (st_contains, xs) = splitConjunctivePredicates(f).partition {
            // TODO: Add guard which checks to see if the function can be pushed down
            case ScalaUDF(_, _, _, _) => true
            case _                              => false
          }
          if(st_contains.nonEmpty) {
            // we got an st_contains, extract the geometry and set up the new GeoMesa relation with the appropriate
            // CQL filter

            // TODO: only dealing with one st_contains at the moment
//            val ScalaUDF(func, _, Seq(GeometryLiteral(_, geom), a), _) = st_contains.head
            val ScalaUDF(func, _, Seq(exprA, exprB), _) = st_contains.head


            // TODO: map func => ff.function
            // TODO: Map Expressions to OpenGIS expressions.
//
//            val b: Expression = a
//            val c: AnyRef = func

            log.warn("Optimizing 'st_contains'")

//            val geomDescriptor = gmRel.sft.getGeometryDescriptor.getLocalName
//            val cqlFilter = ff.contains(ff.property(geomDescriptor), ff.literal(geom))

            val builder: (GTExpression, GTExpression) => org.opengis.filter.Filter = funcToFF(func)
            val gtExprA = sparkExprToGTExpr(exprA)
            val gtExprB = sparkExprToGTExpr(exprB)

            val cqlFilter = builder(gtExprA, gtExprB)

            val relation = gmRel.copy(filt = ff.and(gmRel.filt, cqlFilter))
            // need to maintain expectedOutputAttributes so identifiers don't change in projections
            val newrel = lr.copy(expectedOutputAttributes = Some(lr.output), relation = relation)
            if(xs.nonEmpty) {
              // if there are other filters, keep them
              Filter(xs.reduce(And), newrel)
            } else {
              // if st_contains was the only filter, just return the new relation
              // JNH: I don't believe this should type check.
              newrel
            }
          } else {
            filt
          }
      }
    }

     def funcToFF(func: AnyRef) = {
       func match {
         case ST_Contains => (expr1: GTExpression, expr2: GTExpression) =>
           ff.contains(expr1, expr2)
         case ST_Crosses => (expr1: GTExpression, expr2: GTExpression) =>
           ff.crosses(expr1, expr2)
         case ST_Disjoint => (expr1: GTExpression, expr2: GTExpression) =>
           ff.disjoint(expr1, expr2)
         case ST_Equals => (expr1: GTExpression, expr2: GTExpression) =>
           ff.equal(expr1, expr2)
         case ST_Intersects => (expr1: GTExpression, expr2: GTExpression) =>
           ff.intersects(expr1, expr2)
         case ST_Overlaps => (expr1: GTExpression, expr2: GTExpression) =>
           ff.overlaps(expr1, expr2)
         case ST_Touches => (expr1: GTExpression, expr2: GTExpression) =>
           ff.touches(expr1, expr2)
         case ST_Within => (expr1: GTExpression, expr2: GTExpression) =>
           ff.within(expr1, expr2)
       }
     }

    def sparkExprToGTExpr(expr: org.apache.spark.sql.catalyst.expressions.Expression): org.opengis.filter.expression.Expression = {
      expr match {
        case GeometryLiteral(_, geom) =>
          ff.literal(geom)
        case AttributeReference(name, _, _, _) =>
          ff.property(name)
        case _ =>
          println(s"Got expr: $expr.  Don't know how to turn this into a GeoTools Expression.")
          ff.property("geom")
      }
    }


  }

  object FoldConstantGeometryRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case ScalaUDF(ST_GeomFromWKT, GeometryType, Seq(Literal(wkt, DataTypes.StringType)), Seq(DataTypes.StringType)) =>
            val geom = ST_GeomFromWKT(wkt.asInstanceOf[UTF8String].toString)
            GeometryLiteral(GeometryUDT.serialize(geom), geom)
        }
      }
    }
  }

  def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(FoldConstantGeometryRule, STContainsRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r))
        sqlContext.experimental.extraOptimizations ++= Seq(r)
    }

    Seq.empty[Strategy].foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s))
        sqlContext.experimental.extraStrategies ++= Seq(s)
    }
  }

  def init(sqlContext: SQLContext): Unit = {
    registerFunctions(sqlContext)
    registerOptimizations(sqlContext)
  }
}

//@SQLUserDefinedType
private [spark] class PointUDT extends UserDefinedType[Point] {

  override def simpleString: String = "point"

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Point): InternalRow = {
    new GenericInternalRow(Array(1.asInstanceOf[Byte], UnsafeArrayData.fromPrimitiveArray(Array(obj.getX, obj.getY))))
  }

  override def userClass: Class[Point] = classOf[Point]

  override def deserialize(datum: Any): Point = {
    val ir = datum.asInstanceOf[InternalRow]
    val coords = ir.getArray(1).toDoubleArray()
    SQLTypes.geomFactory.createPoint(new Coordinate(coords(0), coords(1)))
  }
}

object PointUDT extends PointUDT

//@SQLUserDefinedType(LineStringUDT)
private [spark] class LineStringUDT extends UserDefinedType[LineString] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: LineString): InternalRow = {
    // only simple polys for now
    val coords = obj.getCoordinates.map { c => Array(c.x, c.y) }.reduce { (l, r) => l ++ r }
    new GenericInternalRow(Array(2.asInstanceOf[Byte],
      UnsafeArrayData.fromPrimitiveArray(coords)))
  }

  override def userClass: Class[LineString] = classOf[LineString]

  override def deserialize(datum: Any): LineString = {
    val ir = datum.asInstanceOf[InternalRow]
    val coords = ir.getArray(2).toDoubleArray().grouped(2).map { case Array(l, r) => new Coordinate(l, r) }
    SQLTypes.geomFactory.createLineString(coords.toArray)
  }
}

object LineStringUDT extends LineStringUDT

//@SQLUserDefinedType
private [spark] class PolygonUDT extends UserDefinedType[Polygon] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Polygon): InternalRow = {
    // only simple polys for now
    val coords = obj.getCoordinates.map { c => Array(c.x, c.y) }.reduce { (l, r) => l ++ r }
    new GenericInternalRow(Array(3.asInstanceOf[Byte],
      UnsafeArrayData.fromPrimitiveArray(coords)))
  }

  override def userClass: Class[Polygon] = classOf[Polygon]

  override def deserialize(datum: Any): Polygon = {
    val ir = datum.asInstanceOf[InternalRow]
    val coordsD = ir.getArray(1).toDoubleArray()
    val length = coordsD.length
    val numCoords = length/2
    val coords = Array.ofDim[Coordinate](numCoords)
    var i = 0
    while(i < numCoords) {
      val offset = i*2
      coords(i) = new Coordinate(coordsD(offset), coordsD(offset+1))
      i += 1
    }
    SQLTypes.geomFactory.createPolygon(coords)
  }

}

object MultiPolygonUDT extends MultiPolygonUDT

//@SQLUserDefinedType
private [spark] class MultiPolygonUDT extends UserDefinedType[MultiPolygon] {

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.ByteType))
    )
  )
  import org.apache.spark.sql.catalyst.util._

  override def serialize(obj: MultiPolygon): InternalRow = {
    val bytes: Array[Byte] = WKBUtils.write(obj)
    new GenericInternalRow(Array[Any](4.asInstanceOf[Byte], new GenericArrayData(bytes)))
  }

  override def userClass: Class[MultiPolygon] = classOf[MultiPolygon]

  override def deserialize(datum: Any): MultiPolygon = {
    val ir = datum.asInstanceOf[InternalRow]
    WKBUtils.read(ir.getArray(1).toByteArray()).asInstanceOf[MultiPolygon]  // Need cast?
  }

}

object PolygonUDT extends PolygonUDT

//@SQLUserDefinedType
private [spark] class GeometryUDT extends UserDefinedType[Geometry] {


  override def simpleString: String = "geometry"

  override def sqlType: DataType = StructType(
    Seq(
      StructField("type", DataTypes.ByteType),
      StructField("geometry", DataTypes.createArrayType(DataTypes.DoubleType))
    )
  )

  override def serialize(obj: Geometry): InternalRow = {
    obj.getGeometryType match {
      case "Point"      => PointUDT.serialize(obj.asInstanceOf[Point])
      case "LineString" => LineStringUDT.serialize(obj.asInstanceOf[LineString])
      case "Polygon"    => PolygonUDT.serialize(obj.asInstanceOf[Polygon])
      case "MultiPolygon" => MultiPolygonUDT.serialize(obj.asInstanceOf[MultiPolygon])
    }
  }

  override def userClass: Class[Geometry] = classOf[Geometry]

  // JNH: Deal with Multipolygon or something
  override def deserialize(datum: Any): Geometry = {
    val ir = datum.asInstanceOf[InternalRow]
    ir.getByte(0) match {
      case 1 => PointUDT.deserialize(ir)
      case 2 => LineStringUDT.deserialize(ir)
      case 3 => PolygonUDT.deserialize(ir)
      case 4 => MultiPolygonUDT.deserialize(ir)
    }
  }

  private[sql] override def acceptsType(dataType: DataType): Boolean = {
    super.acceptsType(dataType) ||
      dataType.getClass == SQLTypes.PointType.getClass ||
      dataType.getClass == SQLTypes.LineStringType.getClass ||
      dataType.getClass == SQLTypes.PolygonType.getClass ||
      dataType.getClass == SQLTypes.MultipolygonType.getClass
  }
}

case object GeometryUDT extends GeometryUDT
