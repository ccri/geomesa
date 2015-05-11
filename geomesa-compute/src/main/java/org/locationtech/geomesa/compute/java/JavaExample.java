package org.locationtech.geomesa.compute.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.locationtech.geomesa.compute.spark.GeoMesaSpark;
import org.opengis.feature.simple.SimpleFeature;
import scala.runtime.AbstractFunction1;

public class JavaExample {
    public static RDD<Object> loadFromGeoMesaTable(Configuration conf, SparkContext sc) {

        scala.Function1 transform = new AbstractFunction1<SimpleFeature, Tweet>() {
            public Tweet apply(SimpleFeature feature) {
                return new Tweet(feature);
            }
        };

        RDD<Object> rddOut = GeoMesaSpark.rdd(conf, sc, null, null, false)
                .map( transform, scala.reflect.ClassTag$.MODULE$.apply(Tweet.class));

        return rddOut;
    }

}

