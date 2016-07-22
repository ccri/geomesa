/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.ingest

import java.io.{Closeable, File, InputStream}
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.config.{Config, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{Path, Seekable}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptContext, InputSplit, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.convert.Transformers.DefaultCounter
import org.locationtech.geomesa.jobs.mapreduce.AvroFileInputFormat.Counters._
import org.locationtech.geomesa.jobs.mapreduce.{FileStreamRecordReader, ConverterInputFormat, GeoMesaOutputFormat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import com.typesafe.scalalogging.LazyLogging

class ShapefileIngest(dsParams: Map[String, String],
                      inputs: Seq[String],
                      numLocalThreads: Int,
                      sft: SimpleFeatureType,
                      converterConfig: Config) extends AbstractIngest(dsParams, sft.getTypeName, inputs, numLocalThreads) with LazyLogging {
  /**
    * Setup hook - called before run method is executed
    */
  override def beforeRunTasks(): Unit = ???

  /**
    * Create a local ingestion converter
    *
    * @param file file being operated on
    * @param failures used to tracks failures
    * @return local converter
    */
  override def createLocalConverter(file: File, failures: AtomicLong): LocalIngestConverter = ???

  /**
    * Run a distributed ingestion
    *
    * @param statusCallback for reporting status
    * @return (success, failures) counts
    */
  override def runDistributedJob(statusCallback: (Float, Long, Long, Boolean) => Unit): (Long, Long) = ???
}

class ShapefileIngestJob extends AbstractIngestJob {
  override def configureJob(job: Job): Unit = ???

  override def inputFormatClass: Class[_ <: FileInputFormat[_, SimpleFeature]] = ???

  override def written(job: Job): Long = 0L //job.getCounters.findCounter(Group, Read).getValue

  override def failed(job: Job): Long = 0L
}

class ShapefileInputFormat extends FileInputFormat[LongWritable, SimpleFeature] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, SimpleFeature] = ???
}

class ShapefileRecordReader extends FileStreamRecordReader {
  override def createIterator(stream: InputStream with Seekable, filePath: Path, context: TaskAttemptContext): Iterator[SimpleFeature] with Closeable = ???
}