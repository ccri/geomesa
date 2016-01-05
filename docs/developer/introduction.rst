Introduction
============

Project Structure
-----------------

The GeoMesa source distribution is divided into several submodules:

* **geomesa-accumulo**: the implementations of the core Accumulo indexing structures, Accumulo iterators, and the GeoTools interfaces for exposing the functionality as a ``DataStore`` to both application developers and GeoServer.
* **geomesa-assemble**: packages the GeoMesa distributed runtime, GeoMesa GeoServer plugin, and GeoMesa Tools. You can manually assemble using the ``assemble.sh`` script contained in the module.
* **geomesa-compute**: utilities for working with distributed computing environments. Currently, there are methods for instantiating an Apache Spark Resilient Distributed Dataset from a CQL query against data stored in GeoMesa. Eventually, this project will contain bindings for traditional map-reduce processing, Scalding, and other environments.
* **geomesa-convert**: a configurable and extensible library for converting data into SimpleFeatures.
* **geomesa-distributed-runtime**: assembles a jar with dependencies that must be distributed to Accumulo tablet servers lib/ext directory or to an HDFS directory where Accumulo's VFSClassLoader can pick it up.
* **geomesa-examples**: includes Developer quickstart tutorials and examples for how to work with GeoMesa in Accumulo and Kafka.
* **geomesa-features**: includes code for serializing SimpleFeatures and custom SimpleFeature implementations designed for GeoMesa.
* **geomesa-filter**: a library for manipulating and working with GeoTools Filters.
* **geomesa-hbase**: an implementation of GeoMesa on HBase and Google Cloud Bigtable.
* **geomesa-jobs**: map/reduce and scalding jobs for maintaining GeoMesa.
* **geomesa-kafka**: an implementation of GeoMesa in Kafka for maintaining near-real-time caches of streaming data.
* **geomesa-plugin**: creates a plugin which provides WFS and WMS support. The JAR named geomesa-plugin-|version|-geoserver-plugin.jar is ready to be deployed in GeoServer by copying it into ``geoserver/WEB-INF/lib/``.
* **geomesa-process**: analytic processes optimized on GeoMesa data stores.
* **geomesa-raster**: adds support for ingesting and working with geospatially-referenced raster data in GeoMesa.
* **geomesa-security**: adds support for managing security and authorization levels for data stored in GeoMesa. 
* **geomesa-stream**: a GeoMesa library that provides tools to process streams of `SimpleFeatures`.
* **geomesa-tools**: a set of command line tools for managing features, ingesting and exporting data, configuring tables, and explaining queries in GeoMesa.
* **geomesa-utils**: stores our GeoHash implementation and other general library functions unrelated to Accumulo. This sub-project contains any helper tools for geomesa. Some of these tools such as the GeneralShapefileIngest have Map/Reduce components, so the geomesa-utils JAR lives on HDFS.
* **geomesa-web**: web services for accessing GeoMesa.
* **geomesa-z3**: the implementation of Z3, GeoMesa's space-filling Z-order curve.

Installing the Build Tools
--------------------------

TODO

Using Maven
-----------

The GeoMesa project uses `Apache Maven <https://maven.apache.org/>`_ as a build tool. The Maven project's `Maven in 5 Minutes <https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html>`_ provides a quick introduction to getting started with its `mvn` executable.

Using the Scala Console
-----------------------

To test and interact with core functionality, the Scala console can be invoked in a couple of ways. For example, by
running this command in the root source directory:  

    $ cd geomesa-accumulo
    $ mvn -pl geomesa-accumulo-datastore scala:console

The Scala console will start, and all of the project packages in ``geomesa-accumulo-datastore`` will be loaded along
with ``JavaConversions`` and ``JavaConverters``.

Features/SimpleFeatures
-----------------------

TODO

The GeoTools DataStore API
--------------------------

`DataStore <http://docs.geotools.org/latest/userguide/library/api/datastore.html>`_ is the key class in the GeoTools API for accessing data from a source such as GeoMesa. 

WPS
---

As described by the Open Geospatial Consortium's page on WPS, 

    The OpenGIS® Web Map Service Interface Standard (WMS) provides a simple HTTP
    interface for requesting geo-registered map images from one or more
    distributed geospatial databases. A WMS request defines the geographic
    layer(s) and area of interest to be processed. The response to the request is
    one or more geo-registered map images (returned as JPEG, PNG, etc) that can be
    displayed in a browser application. The interface also supports the ability to
    specify whether the returned images should be transparent so that layers from
    multiple servers can be combined or not.

A tool like GeoServer (once its WPS plugin has been installed) uses WPS to retrieve data from GeoMesa. WPS processes can be chained, letting you use additional WPS requests to build on the results of earlier ones. The `Web Processing Services (WPS) Tube Select <../../tutorials/html/geomesa-tubeselect.html>`_ tutorial describes how to assemble an application that does this. 
