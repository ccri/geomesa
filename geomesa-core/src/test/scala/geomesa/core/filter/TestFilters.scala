package geomesa.core.filter

import geomesa.core.filter.FilterUtils._
import geomesa.core.index.IndexSchema
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, Interval}
import org.opengis.filter.Filter
import org.opengis.filter._
import scala.collection.JavaConversions._

object TestFilters {

  val baseFilters: Seq[Filter] =
    Seq(
      "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
      "INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "NOT (INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "NOT (INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "attr56 = val56",
      "dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'",
      "dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'"
    )

  val oneLevelAndFilters: Seq[Filter] =
    Seq(
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND attr17 = val17)",
      "(INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))",
      "(INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND attr81 = val81)",
      "(attr15 = val15 AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))"
    )

  val oneLevelOrFilters: Seq[Filter] =
    Seq(
      "(INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "(INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR attr4 = val4)",
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr20 = val20)",
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr36 = val36)",
      "(INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr24 = val24)",
      "(attr100 = val100 OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr54 = val54)",
      "(attr19 = val19 OR attr75 = val75 OR attr72 = val72)",
      "(attr37 = val37 OR attr19 = val19)",
      "(attr44 = val44 OR INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr32 = val32)",
      "(attr95 = val95 OR INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))"
    )

  val simpleNotFilters: Seq[Filter] =
    Seq(
      "NOT (INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))))",
      "NOT (INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))))",
      "NOT (INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "NOT (attr23 = val23)",
      "NOT (attr89 = val89)",
      "NOT (dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "NOT (dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')"
    )

  val andsOrsFilters: Seq[Filter] =
    Seq(
      "((INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))) AND (dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR attr22 = val22 OR attr86 = val86) AND (dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z'))",
      "((dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))) OR (attr85 = val85 OR dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') OR (INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z'))",
      "(dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' OR attr31 = val31 OR dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' OR dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "((attr32 = val32 AND dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') AND (INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) OR attr82 = val82 OR INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))))",
      "((attr44 = val44 AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))) OR (INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR attr2 = val2))",
      "(dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR attr51 = val51 OR attr39 = val39)"
    )

  val oneGeomFilters: Seq[Filter] =
    Seq(
      "((dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z') OR (dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))))",
      "(INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) AND dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23))) OR dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' OR dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z')",
      "(attr84 = val84 AND dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))",
      "(dtg BETWEEN '0000-01-01T00:00:00.000Z' AND '9999-12-31T23:59:59.000Z' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z')",
      "(dtg BETWEEN '2010-07-01T00:00:00.000Z' AND '2010-07-31T00:00:00.000Z' AND attr92 = val92 AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))))"
    )

  val goodSpatialPredicates =
    Seq(
      "INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "OVERLAPS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "CONTAINS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
      "CROSSES(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
    )

  val andedSpatialPredicates = Seq(
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) AND DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))"
  )

  val oredSpatialPredicates = Seq(
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "WITHIN(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "DISJOINT(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR CROSSES(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR INTERSECTS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR OVERLAPS(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR WITHIN(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))",
    "CROSSES(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28))) OR DISJOINT(geom, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))"
  )

  val attributePredicates = Seq(
    "attr2 = '2nd100001'",
    "attr2 ILIKE '%1'",
    "attr2 ILIKE '2nd1%'",
    "attr2 ILIKE '1%'"      // Returns 0 since medium data features start with "2nd"
  )

  val attributeAndGeometricPredicates = Seq(
    "attr2 = '2nd100001' AND DISJOINT(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    // For mediumData, this next filter will hit and the one after will not.
    "attr2 = '2nd100001' AND INTERSECTS(geom, POLYGON ((45 20, 48 20, 48 27, 45 27, 45 20)))",
    "attr2 = '2nd100001' AND INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "attr2 ILIKE '%1' AND CROSSES(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "attr2 ILIKE '%1' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "attr2 ILIKE '%1' AND OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "attr2 ILIKE '%1' AND WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "attr2 ILIKE '2nd1%' AND CROSSES(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "attr2 ILIKE '2nd1%' AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "attr2 ILIKE '2nd1%' AND OVERLAPS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "attr2 ILIKE '2nd1%' AND WITHIN(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
  )

  val temporalPredicates = Seq(
  "(not dtg after 2010-08-08T23:59:59Z) and (not dtg_end_time before 2010-08-08T00:00:00Z)",
  "(dtg between '2010-08-08T00:00:00.000Z' AND '2010-08-08T23:59:59.000Z')"
  )

}
