.. _geomesa-installation:

GeoMesa Installation
====================

These instructions show how to:

1. Install the GeoMesa command line tools.
2. Deploy the distributed runtime JAR to your Accumulo cluster.
3. Deploy the GeoServer plugin.
4. Deploy necessary dependencies for GeoMesa's GeoServer plugin for
   Accumulo and Kafka.

Prerequisites
-------------

.. warning::

    For Accumulo deployment, you will need access to a Hadoop 2.2 installation as well as an Accumulo |accumulo_version| database.

Before you begin, you should have these:

-  basic knowledge of `GeoTools <http://www.geotools.org>`__,
   `GeoServer <http://geoserver.org>`__,
   `Accumulo <http://accumulo.apache.org>`__, and/or
   `Kafka <http://kafka.apache.org>`__,
-  access to an Accumulo |accumulo_version| database (requires a
   Hadoop 2.2 installation),
-  access to a `GeoServer <http://geoserver.org/>`__ 2.5.2 installation,
-  an Accumulo user that has both create-table and write permissions,
   and
-  `Java 7
   JRE <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__
   installed.

Download the GeoMesa Binary Distribution
----------------------------------------

GeoMesa artifacts are available for download or can be built from
source. The easiest way to get started is to download the `*-bin.tar.gz` file of the most recent
stable version (|version|)  from that subdirectory of http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-assemble/ and untar it somewhere convenient. The following, and additional command lines shown in these instructions, assume that $VERSION has been set to |version|:

.. code-block:: bash

    # cd to a convenient directory for installing geomesa
    $ cd ~/tools

    # download and unpackage the most recent distribution
    $ wget http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-assemble/$VERSION/geomesa-assemble-$VERSION-bin.tar.gz
    $ tar xvf geomesa-assemble-$VERSION-bin.tar.gz
    $ cd geomesa-$VERSION
    $ ls
    bin  dist  docs  lib  LICENSE.txt  README.md

Install the Command Line Tools
------------------------------

GeoMesa comes with a set of command line tools for managing features. To
complete the setup of the tools, cd into the bin directory and execute
geomesa configure:

.. code-block:: bash

    $ cd ~/tools/geomesa-$VERSION/bin
    $ ./geomesa configure
    Warning: GEOMESA_HOME is not set, using ~/tools/geomesa-$VERSION
    Using GEOMESA_HOME as set: ~/tools/geomesa-$VERSION
    Is this intentional? Y\n Y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
     Current value is ~/tools/geomesa-$VERSION/lib.

    Is this intentional? Y\n Y

    To persist the configuration please update your bashrc file to include:
    export GEOMESA_HOME=/tools/geomesa-$VERSION
    export PATH=${GEOMESA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the
$GEOMESA\_HOME and $PATH updates.

Install GPL software:

.. code-block:: bash

    $ bin/install-jai
    $ bin/install-jline
    $ bin/install-vecmath

Finally, edit the configuration variables at the top of the
``bin/test-geomesa`` script as appropriate for your server
configuration, and run it to test your installation:

.. code-block:: bash

    $ bin/test-geomesa

Test the GeoMesa Tools:

.. code-block:: bash

    $ geomesa
    Using GEOMESA_HOME = /path/to/geomesa-$VERSION
    Usage: geomesa [command] [command options]
      Commands:
        create           Create a feature definition in a GeoMesa catalog
        deletecatalog    Delete a GeoMesa catalog completely (and all features in it)
        deleteraster     Delete a GeoMesa Raster Table
        describe         Describe the attributes of a given feature in GeoMesa
        explain          Explain how a GeoMesa query will be executed
        export           Export a GeoMesa feature
        getsft           Get the SimpleFeatureType of a feature
        help             Show help
        ingest           Ingest a file of various formats into GeoMesa
        ingestraster     Ingest a raster file or raster files in a directory into GeoMesa
        list             List GeoMesa features for a given catalog
        querystats       Export queries and statistics about the last X number of queries to a CSV file.
        removeschema     Remove a schema and associated features from a GeoMesa catalog
        tableconf        Perform table configuration operations
        version          GeoMesa Version

For more information on the tools check out the `GeoMesa Tools
tutorial </geomesa-tools-features/>`__ after you're done with this
tutorial.

GeoMesa Tools comes with a bundled SLF4J implementation. However, if you
receive an SLF4J error like this:

.. code-block:: bash

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

download the SLF4J TAR-ball `found
here <http://www.slf4j.org/download.html>`__. Extract
slf4j-log4j12-1.7.7.jar and place it in the geomesa-|version|/lib directory.

If this conflicts with another SLF4J implementation, it may need to be
removed from the lib directory.

Deploy GeoMesa to Accumulo
--------------------------

The $GEOMESA\_HOME/dist directory contains the distributed runtime jar
that should be copied into the $ACCUMULO\_HOME/lib/ext folder on each
tablet server. This jar contains the GeoMesa Accumulo iterators that are
necessary to query GeoMesa.

.. code-block:: bash

    # something like this for each tablet server
    scp $GEOMESA_HOME/dist/geomesa-distributed-runtime-$VERSION.jar tserver1:$ACCUMULO_HOME/lib/ext/

Deploy GeoMesa Accumulo Plugin to GeoServer
-------------------------------------------

You should have an instance of GeoServer, version 2.5.2, running
somewhere that has access to your Accumulo instance.

GeoServer Setup
~~~~~~~~~~~~~~~

First, you will need to install the WPS plugin to your GeoServer
instance. The `WPS
Plugin <http://docs.geoserver.org/stable/en/user/extensions/wps/install.html>`__
must also match the version of GeoServer instance.

Copy the
``geomesa-plugin-$VERSION-geoserver-plugin.jar`` jar
file from the GeoMesa dist directory into your GeoServer's library
directory.

If you are using tomcat:

.. code-block:: bash

    cp $GEOMESA_HOME/dist/geomesa-plugin-$VERSION-geoserver-plugin.jar /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    cp $GEOMESA_HOME/dist/geomesa-plugin-$VERSION-geoserver-plugin.jar /path/to/geoserver-2.5.2/webapps/geoserver/WEB-INF/lib/

Additional dependencies
~~~~~~~~~~~~~~~~~~~~~~~

There are additional JARs that are specific to your installation that
you will also need to copy to GeoServer's ``WEB-INF/lib`` directory.
There is a script located at
``$GEOMESA_HOME/bin/install-hadoop-accumulo.sh`` which will install
these dependencies to a target directory using ``wget`` which will
require an internet connection. In the source distribution this script
is found at ``./geomesa-tools/bin/install-hadoop-accumulo.sh``.

For example:

.. code-block:: bash

    $> $GEOMESA_HOME/bin/install-hadoop-accumulo.sh /path/to/tomcat/webapps/geoserver/WEB-INF/lib/
    Install accumulo and hadoop dependencies to /path/to/tomcat/webapps/geoserver/WEB-INF/lib/?
    Confirm? [Y/n]y
    fetching https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar
    --2015-09-29 15:06:48--  https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar
    Resolving search.maven.org (search.maven.org)... 207.223.241.72
    Connecting to search.maven.org (search.maven.org)|207.223.241.72|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 4646545 (4.4M) [application/java-archive]
    Saving to: ‘/path/to/tomcat/webapps/geoserver/WEB-INF/lib/accumulo-core-1.6.2.jar’
    ...

If you do no have an internet connection you can download the jars
manually. These may include (the specific JARs are included only for
reference, and only apply if you are using Accumulo 1.6.2 and Hadoop
2.2):

-  Accumulo

   -  accumulo-core-1.6.2.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar>`__
   -  accumulo-fate-1.6.2.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-fate/1.6.2/accumulo-fate-1.6.2.jar>`__
   -  accumulo-trace-1.6.2.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-trace/1.6.2/accumulo-trace-1.6.2.jar>`__

-  Zookeeper

   -  zookeeper-3.4.5.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/3.4.5/zookeeper-3.4.5.jar>`__

-  Hadoop core

   -  hadoop-auth-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-auth/2.2.0/hadoop-auth-2.2.0.jar>`__
   -  hadoop-client-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-client/2.2.0/hadoop-client-2.2.0.jar>`__
   -  hadoop-common-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-common/2.2.0/hadoop-common-2.2.0.jar>`__
   -  hadoop-hdfs-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-hdfs/2.2.0/hadoop-hdfs-2.2.0.jar>`__
   -  hadoop-mapreduce-client-app-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-app/2.2.0/hadoop-mapreduce-client-app-2.2.0.jar>`__
   -  hadoop-mapreduce-client-common-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-common/2.2.0/hadoop-mapreduce-client-common-2.2.0.jar>`__
   -  hadoop-mapreduce-client-core-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-core/2.2.0/hadoop-mapreduce-client-core-2.2.0.jar>`__
   -  hadoop-mapreduce-client-jobclient-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-jobclient/2.2.0/hadoop-mapreduce-client-jobclient-2.2.0.jar>`__
   -  hadoop-mapreduce-client-shuffle-2.2.0.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-shuffle/2.2.0/hadoop-mapreduce-client-shuffle-2.2.0.jar>`__

-  Thrift

   -  libthrift-0.9.1.jar
      `[download] <https://search.maven.org/remotecontent?filepath=org/apache/thrift/libthrift/0.9.1/libthrift-0.9.1.jar>`__

There are also GeoServer JARs that need to be updated for Accumulo (also
in the lib directory):

-  commons-configuration: Accumulo requires commons-configuration 1.6
   and previous versions should be replaced
   `[download] <https://search.maven.org/remotecontent?filepath=commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar>`__
-  commons-lang: GeoServer ships with commons-lang 2.1, but Accumulo
   requires replacing that with version 2.4
   `[download] <https://search.maven.org/remotecontent?filepath=commons-lang/commons-lang/2.4/commons-lang-2.4.jar>`__

Once all of the dependencies for the GeoServer plugin are in place you
will need to restart GeoServer for the changes to take effect.

Verify Deployment
~~~~~~~~~~~~~~~~~

To verify that the deployment worked you can follow the `GeoMesa Quick
Start tutorial </geomesa-quickstart/>`__ to ingest test data and view
the data in GeoServer.

Deploy GeoMesa to Kafka and GeoServer
-------------------------------------

.. raw:: html

   <div class="callout callout-warning">

::

    <span class="glyphicon glyphicon-exclamation-sign"></span>
    Building the Kafka submodule requires that development tools be installed and configured.

.. raw:: html

   </div>

These development tools are required:

-  `Java JDK
   7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`__,
-  `Apache Maven <http://maven.apache.org/>`__ 3.2.2 or better, and
-  `Git <https://git-scm.com/>`__.

Building Kafka Support
~~~~~~~~~~~~~~~~~~~~~~

To set up GeoMesa with Kafka, download the GeoMesa source distribution
that matches the binary distribution described above:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa/
    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION

Then build the geomesa-kafka submodule (see the `Kafka Quickstart
tutorial </geomesa-kafka-quickstart/>`__ to see what GeoMesa can do with
Kafka).

.. code-block:: bash

    $ mvn clean install -f geomesa/geomesa-kafka/pom.xml -DskipTests

Installing GeoMesa Kafka plugin in GeoServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, you will need to install the WPS plugin to your GeoServer
instance if you have not done so already. The `WPS
Plugin <http://docs.geoserver.org/stable/en/user/extensions/wps/install.html>`__
must also match the version of the GeoServer instance.

Copy the GeoMesa Kafka plugin JAR files from the GeoMesa directory you
built into your GeoServer's library directory.

Tomcat:

.. code-block:: bash

    cp geomesa/geomesa-kafka/geomesa-kafka-geoserver-plugin/target/geomesa-kafka-geoserver-plugin-$VERSION-geoserver-plugin.jar /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

Jetty:

.. code-block:: bash

    cp geomesa/geomesa-kafka/geomesa-kafka-geoserver-plugin/target/geomesa-kafka-geoserver-plugin-$VERSION-geoserver-plugin.jar /path/to/jetty/geoserver-2.5.2/webapps/geoserver/WEB-INF/lib/

Then copy these dependencies to your ``WEB-INF/lib`` directory.

-  Kafka

   -  kafka-clients-0.8.2.1.jar
   -  kafka\_2.10-0.8.2.1.jar
   -  metrics-core-2.2.0.jar
   -  zkclient-0.3.jar

-  Zookeeper

   -  zookeeper-3.4.5.jar

Note: when using the Kafka Data Store with GeoServer in Tomcat it will
most likely be necessary to increase the memory settings for Tomcat,
``export CATALINA_OPTS="-Xms512M -Xmx1024M -XX:PermSize=256m -XX:MaxPermSize=256m"``.

After placing the dependencies in the correct folder, be sure to restart
GeoServer for changes to take place.

Configuring GeoServer
---------------------

Depending on your hardware, it may be important to set the limits for
your WMS plugin to be higher or disable them completely by clicking
"WMS" under "Services" on the left side of the admin page of GeoServer.
Check with your server administrator to determine the correct settings.
For massive queries, the standard 60 second timeout may be too short.

|"Disable limits"|

.. |"Disable limits"| image:: _static/img/wms_limits.png
