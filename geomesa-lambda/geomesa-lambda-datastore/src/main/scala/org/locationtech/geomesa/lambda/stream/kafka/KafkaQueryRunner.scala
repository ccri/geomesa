/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.nio.charset.StandardCharsets

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.filter.visitor.BindingFilterVisitor
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.{EncodingOptions, GeometryAttribute}
import org.locationtech.geomesa.filter.function.{AxisOrder, BinaryOutputEncoder}
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils}
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityUtils, VisibilityEvaluator}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.{Filter, Id}

class KafkaQueryRunner(features: SharedState, authProvider: Option[AuthorizationsProvider])
    extends QueryRunner {

  import KafkaQueryRunner.{authVisibilityCheck, noAuthVisibilityCheck}
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val isVisible: (SimpleFeature, Seq[Array[Byte]]) => Boolean =
    if (authProvider.isDefined) { authVisibilityCheck } else { noAuthVisibilityCheck }

  override def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._

    val auths = authProvider.map(_.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8))).getOrElse(Seq.empty)

    val query = configureQuery(sft, original)
    optimizeFilter(sft, query)

    explain.pushLevel(s"Kafka lambda query: '${sft.getTypeName}' ${org.locationtech.geomesa.filter.filterToString(query.getFilter)}")
    explain(s"bin[${query.getHints.isBinQuery}] arrow[${query.getHints.isArrowQuery}] " +
        s"density[${query.getHints.isDensityQuery}] stats[${query.getHints.isStatsQuery}]")
    explain(s"Transforms: ${query.getHints.getTransformDefinition.getOrElse("None")}")
    explain(s"Sort: ${Option(query.getSortBy).filter(_.nonEmpty).map(_.mkString(", ")).getOrElse("none")}")
    explain.popLevel()

    // just iterate through the features
    // we could use an in-memory index here if performance isn't good enough

    val iter = Option(query.getFilter).filter(_ != Filter.INCLUDE) match {
      case Some(f: Id) => f.getIDs.iterator.map(i => features.get(i.toString)).filter(sf => sf != null && isVisible(sf, auths))
      case Some(f)     => features.all().filter(sf => f.evaluate(sf) && isVisible(sf, auths))
      case _           => features.all().filter(sf => isVisible(sf, auths))
    }

    CloseableIterator(transform(sft, query.getHints, iter))
  }

  private def transform(sft: SimpleFeatureType,
                        hints: Hints,
                        features: Iterator[SimpleFeature]): Iterator[SimpleFeature] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    if (hints.isBinQuery) {
      val trackId = Option(hints.getBinTrackIdField)
      val geom = hints.getBinGeomField.orElse(Option(sft.getGeomField)).map(GeometryAttribute(_, AxisOrder.LonLat))
      val dtg = hints.getBinDtgField
      val label = hints.getBinLabelField

      val encode = BinaryOutputEncoder.encodeFeatures(sft, EncodingOptions(geom, dtg, trackId, label))

      val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft, Array(null, GeometryUtils.zeroPoint))
      features.map { feature =>
        sf.setAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX, encode(feature))
        sf
      }
    } else if (hints.isArrowQuery) {
      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues
      //      if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
      //          dictionaryFields.forall(providedDictionaries.contains)) {
      //        // TODO this will end up returning two files, is that ok?
      //        val dictionaries = ArrowBatchScan.createDictionaries(this, sft, Option(query.getFilter), dictionaryFields, providedDictionaries)
      //        val iter = ArrowBatchIterator.configure(sft, this, ecql, dictionaries, hints, dedupe)
      //        val reduce = Some(ArrowBatchScan.reduceFeatures(hints.getTransformSchema.getOrElse(sft), hints, dictionaries))
      //        ScanConfig(Seq(iter), FullColumnFamily, ArrowBatchIterator.kvsToFeatures(), reduce)
      //      } else {
      //        val iter = ArrowFileIterator.configure(sft, this, ecql, dictionaryFields, hints, dedupe)
      //        ScanConfig(Seq(iter), FullColumnFamily, ArrowFileIterator.kvsToFeatures(), None)
      //      }
      org.locationtech.geomesa.arrow.ArrowEncodedSft
      ???
    } else if (hints.isDensityQuery) {
      val Some(envelope) = hints.getDensityEnvelope
      val Some((width, height)) = hints.getDensityBounds
      val grid = new GridSnap(envelope, width, height)
      val result = scala.collection.mutable.Map.empty[(Int, Int), Double]
      val getWeight = DensityScan.getWeight(sft, hints.getDensityWeight)
      val writeGeom = DensityScan.writeGeometry(sft, grid)
      features.foreach(f => writeGeom(f, getWeight(f), result))

      val sf = new ScalaSimpleFeature("", DensityScan.DensitySft, Array(GeometryUtils.zeroPoint))
      // Return value in user data so it's preserved when passed through a RetypingFeatureCollection
      sf.getUserData.put(DensityScan.DensityValueKey, DensityScan.encodeResult(result))
      Iterator(sf)
    } else if (hints.isStatsQuery) {
      val stat = Stat(sft, hints.getStatsQuery)
      features.foreach(stat.observe)
      val encoded = if (hints.isStatsEncode) { KryoLazyStatsUtils.encodeStat(sft)(stat) } else { stat.toJson }
      Iterator(new ScalaSimpleFeature("stat", KryoLazyStatsUtils.StatsSft, Array(encoded, GeometryUtils.zeroPoint)))
    } else {
      hints.getTransform match {
        case None => features
        case Some((definitions, transformSchema)) =>
          val tdefs = TransformProcess.toDefinition(definitions)
          val reusableSf = new ScalaSimpleFeature("", transformSchema)
          var i = 0
          features.map { feature =>
            reusableSf.setId(feature.getID)
            while (i < tdefs.size) {
              reusableSf.setAttribute(i, tdefs.get(i).expression.evaluate(feature))
              i += 1
            }
            i = 0
            reusableSf
          }
      }
    }
  }

  override protected def optimizeFilter(sft: SimpleFeatureType, filter: Filter): Filter = {
    FastFilterFactory.sfts.set(sft)
    try {
      filter.accept(new BindingFilterVisitor(sft), null).asInstanceOf[Filter]
          .accept(new QueryPlanFilterVisitor(sft), FastFilterFactory.factory).asInstanceOf[Filter]
    } finally {
      FastFilterFactory.sfts.remove()
    }
  }

  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      KryoLazyStatsUtils.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}

object KafkaQueryRunner {

  private def noAuthVisibilityCheck(f: SimpleFeature, ignored: Seq[Array[Byte]]): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || vis.isEmpty
  }

  private def authVisibilityCheck(f: SimpleFeature, auths: Seq[Array[Byte]]): Boolean = {
    val vis = SecurityUtils.getVisibility(f)
    vis == null || VisibilityEvaluator.parse(vis).evaluate(auths)
  }
}