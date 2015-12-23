Getting Started
===============

Prerequisites and Platform
--------------------------

.. warning::

    GeoMesa requires Accumulo (http://accumulo.apache.org/) |accumulo_version|, which in turn
    requires Hadoop (http://hadoop.apache.org/) |hadoop_version| and ZooKeeper (http://zookeeper.apache.org) 
    |zookeeper_version|. Installing and configuring Accumulo is beyond the scope of this manual.

    Using the Kafka (http://kafka.apache.org/) module requires Kafka |kafka_version| and ZooKeeper |zookeeper_version|.  

To install the binary distribution:

* Java JRE or JDK 7

To build and install the source distribution:

* Java JDK 7
* Apache Maven (http://maven.apache.org/) |maven_version|
* A ``git`` client (http://git-scm.com/)

Versions and Downloads
----------------------

.. note::

    The current recommended version is |release|.

**Latest release**: |release|

.. TODO: substitutions don't work in some kinds of markup, including URLs

* Release tarball: |release_tarball|
* Source: |release_source_tarball|

**Development version (source only)**: |development|

* Source: https://github.com/locationtech/geomesa/archive/master.tar.gz

**1.0.x release**: geomesa-accumulo1.5-1.0.0-rc.7 

* Source: https://github.com/locationtech/geomesa/releases/tag/geomesa-accumulo1.5-1.0.0-rc.7
* Release tarball: https://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-assemble-accumulo1.5/1.0.0-rc.7/geomesa-assemble-accumulo1.5-1.0.0-rc.7-bin.tar.gz

GeoMesa artifacts can be downloaded from the LocationTech Maven repository: https://repo.locationtech.org/content/repositories/geomesa-releases/.

Snapshots are available in the LocationTech Snapshots Repositor: https://repo.locationtech.org/content/repositories/geomesa-snapshots/.

.. _install_binary:

Installing from the Binary Distribution
---------------------------------------

GeoMesa artifacts are available for download or can be built from source. 
The easiest way to get started is to download the most recent binary version (``$VERSION`` = |release|) 
and untar it somewhere convenient.

.. code-block:: bash

    # cd to a convenient directory for installing geomesa 
    $ cd ~/tools

    # download and unpackage the most recent distribution
    $ wget http://repo.locationtech.org/content/repositories/geomesa-releases/org/locationtech/geomesa/geomesa-assemble/$VERSION/geomesa-assemble-$VERSION-bin.tar.gz
    $ tar xvf geomesa-assemble-$VERSION-bin.tar.gz
    $ cd geomesa-$VERSION
    $ ls
    bin  dist  docs  lib  LICENSE.txt  README.md

.. _building_source:

Building from Source
--------------------

These development tools are required:

* `Java JDK 7 <http://www.oracle.com/technetwork/java/javase/downloads/index.html>`_,
* `Apache Maven <http://maven.apache.org/>`_ 3.2.2 or better, and
* `Git <https://git-scm.com/>`_.

The GeoMesa source distribution may be cloned from GitHub:

.. code-block:: bash

    $ git clone https://github.com/locationtech/geomesa.git
    $ cd geomesa

This downloads the latest development version. To check out the code for the latest stable release
(``$VERSION`` = |release|):

.. code-block:: bash

    $ git checkout tags/geomesa-$VERSION -b geomesa-$VERSION 


Building and dependency management for GeoMesa is handled by Maven (http://maven.apache.org/). 
The Maven ``pom.xml`` file in the root directory of the source distribution contains an explicit
list of dependent libraries that will be bundled together for each module of the program.

.. note::

    The only reason dependent libraries are bundled into the final JAR is to make it easier 
    to deploy files rather than setting the classpath. If you would rather not bundle these 
    dependencies, mark them as "provided" in the POM, and update your classpath as appropriate.

The version of Accumulo supported is controlled by the `accumulo-1.5` 
property; to target Accumulo 1.5:   

.. code-block:: bash

    $ mvn clean install -Daccumulo-1.5

If the property is omitted, support for Accumulo 1.6 is assumed:

.. code-block:: bash

    $ mvn clean install

The `skipTests` property may be used to speed compilation. Set it to ``true``
to omit the test phase of the build process:

.. code-block:: bash

    $ mvn clean install -DskipTests=true

The ``build/mvn`` script is a wrapper around Maven that builds the project using the Zinc
(https://github.com/typesafehub/zinc) incremental compiler:

.. code-block:: bash

    $ build/mvn clean install -Daccumulo-1.5  # Accumulo 1.5
    $ build/mvn clean install                 # Accumulo 1.6

Setting up the Command Line Tools
---------------------------------

.. note::

    The instructions for setting up the command line tools assumes the use of the 
    binary GeoMesa distribution. If you have built GeoMesa from source (see :ref:`building_source`),
    use the ``geomesa-assemble/assemble.sh`` script to bundle a tarball of the binary
    distribution.

GeoMesa comes with a set of command line tools for managing features. To complete the setup 
of the tools, cd into the ``bin`` directory of the binary distribution and execute 
``geomesa configure``:

.. code-block:: bash

    $ cd /path/to/geomesa-$VERSION/bin
    $ ./geomesa configure
    Warning: GEOMESA_HOME is not set, using /path/to/geomesa-$VERSION
    Using GEOMESA_HOME as set: /path/to/geomesa-$VERSION
    Is this intentional? Y\n Y
    Warning: GEOMESA_LIB already set, probably by a prior configuration.
    Current value is /path/to/geomesa-$VERSION/lib.

    Is this intentional? Y\n Y

    To persist the configuration please update your bashrc file to include: 
    export GEOMESA_HOME=/path/to/geomesa-$VERSION
    export PATH=${GEOMESA_HOME}/bin:$PATH

Update and re-source your ``~/.bashrc`` file to include the $GEOMESA_HOME and $PATH updates.

Install GPL software:

.. code-block:: bash

    $ bin/install-jai
    $ bin/install-jline
    $ bin/install-vecmath

Finally, test your installation by editing the ``bin/test-geomesa`` file with configuration
data specific to your setup and running it: 

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

GeoMesa Tools comes with a bundled SLF4J implementation. However, if you receive an SLF4J error like this:

.. code-block:: bash

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
    
download the SLF4J TAR file from http://www.slf4j.org/download.html. Extract 
``slf4j-log4j12-1.7.7.jar`` and place it in the ``lib`` directory of the binary distribution. 

If this conflicts with another SLF4J implementation, it may need to be removed from the lib directory.


Configuring for Accumulo
------------------------

The ``$GEOMESA_HOME/dist`` directory contains the distributed runtime jar that should be copied into the ``$ACCUMULO_HOME/lib/ext`` folder on each tablet server. This jar contains the GeoMesa Accumulo iterators that are necessary to query GeoMesa.

.. code-block:: bash

    # something like this for each tablet server
    $ scp $GEOMESA_HOME/dist/geomesa-distributed-runtime-$GEOMESA_VERSION.jar tserver1:$ACCUMULO_HOME/lib/ext/


Configuring for Kafka
---------------------

To install the Kafka module, you will need to build GeoMesa from source (see :ref:`building_source`). 
Build the ``geomesa-kafka`` submodule:

.. code-block:: bash

    $ mvn clean install -f geomesa/geomesa-kafka/pom.xml -DskipTests


Installing the GeoMesa GeoServer plugin
---------------------------------------

In addition to the GeoServer plugin, you will also need to install the WPS plugin to your GeoServer
instance. The GeoServer WPS Plugin (available at 
http://docs.geoserver.org/stable/en/user/extensions/wps/install.html) must also match the version of
GeoServer instance. This is needed for both the Accumulo and Kafka variants of the plugin.

For Accumulo
^^^^^^^^^^^^

.. note:: 

    ``$VERSION`` = |release| unless you have built from source.

Copy the ``geomesa-$VERSION-geoserver-plugin.jar`` jar file from the GeoMesa dist directory into your GeoServer's library directory.

If you are using tomcat:

.. code-block:: bash

    $ cp $GEOMESA_HOME/dist/geomesa-plugin-$VERSION-geoserver-plugin.jar /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ cp $GEOMESA_HOME/dist/geomesa-plugin-$VERSION-geoserver-plugin.jar /path/to/geoserver-2.5.2/webapps/geoserver/WEB-INF/lib/

There are additional JARs that are specific to your installation that you will also need to 
copy to GeoServer's ``WEB-INF/lib`` directory. There is a script located at 
``$GEOMESA_HOME/bin/install-hadoop-accumulo.sh`` which will install these dependencies to a
target directory using ``wget`` which will require an internet connection. 

For example:

.. code-block:: bash

    $ $GEOMESA_HOME/bin/install-hadoop-accumulo.sh /path/to/tomcat/webapps/geoserver/WEB-INF/lib/
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

If you do no have an internet connection you can download the JARs manually via http://search.maven.org/.
These may include (the specific JARs are included only for reference, and only apply if you are using Accumulo 1.6.2 and Hadoop 2.2):

* Accumulo
    * `accumulo-core-1.6.2.jar <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-core/1.6.2/accumulo-core-1.6.2.jar>`_
    * `accumulo-fate-1.6.2.jar <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-fate/1.6.2/accumulo-fate-1.6.2.jar>`_
    * `accumulo-trace-1.6.2.jar <https://search.maven.org/remotecontent?filepath=org/apache/accumulo/accumulo-trace/1.6.2/accumulo-trace-1.6.2.jar>`_
* Zookeeper
    * `zookeeper-3.4.5.jar <https://search.maven.org/remotecontent?filepath=org/apache/zookeeper/zookeeper/3.4.5/zookeeper-3.4.5.jar>`_
* Hadoop core
    * `hadoop-auth-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-auth/2.2.0/hadoop-auth-2.2.0.jar>`_
    * `hadoop-client-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-client/2.2.0/hadoop-client-2.2.0.jar>`_
    * `hadoop-common-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-common/2.2.0/hadoop-common-2.2.0.jar>`_
    * `hadoop-hdfs-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-hdfs/2.2.0/hadoop-hdfs-2.2.0.jar>`_
    * `hadoop-mapreduce-client-app-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-app/2.2.0/hadoop-mapreduce-client-app-2.2.0.jar>`_
    * `hadoop-mapreduce-client-common-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-common/2.2.0/hadoop-mapreduce-client-common-2.2.0.jar>`_
    * `hadoop-mapreduce-client-core-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-core/2.2.0/hadoop-mapreduce-client-core-2.2.0.jar>`_
    * `hadoop-mapreduce-client-jobclient-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-jobclient/2.2.0/hadoop-mapreduce-client-jobclient-2.2.0.jar>`_
    * `hadoop-mapreduce-client-shuffle-2.2.0.jar <https://search.maven.org/remotecontent?filepath=org/apache/hadoop/hadoop-mapreduce-client-shuffle/2.2.0/hadoop-mapreduce-client-shuffle-2.2.0.jar>`_
* Thrift
    * `libthrift-0.9.1.jar <https://search.maven.org/remotecontent?filepath=org/apache/thrift/libthrift/0.9.1/libthrift-0.9.1.jar>`_
    
There are also GeoServer JARs that need to be updated for Accumulo (also in the lib directory):
    
* **commons-configuration**: Accumulo requires commons-configuration 1.6 and previous versions should be replaced [`commons-configuration-1.6.jar <https://search.maven.org/remotecontent?filepath=commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar>`_]
* **commons-lang**: GeoServer ships with commons-lang 2.1, but Accumulo requires replacing that with version 2.4 [`commons-lang-2.4.jar <https://search.maven.org/remotecontent?filepath=commons-lang/commons-lang/2.4/commons-lang-2.4.jar>`_]

For Kafka
^^^^^^^^^

Copy the GeoMesa Kafka plugin JAR files from the GeoMesa directory you built into your GeoServer's library directory. 

Tomcat::

    $ cp geomesa/geomesa-kafka/geomesa-kafka-geoserver-plugin/target/geomesa-kafka-geoserver-plugin-$VERSION-geoserver-plugin.jar /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

GeoServer's built-in Jetty web server::

    $ cp geomesa/geomesa-kafka/geomesa-kafka-geoserver-plugin/target/geomesa-kafka-geoserver-plugin-$VERSION-geoserver-plugin.jar /path/to/jetty/geoserver-2.5.2/webapps/geoserver/WEB-INF/lib/

Then copy these dependencies to your `WEB-INF/lib` directory.

* Kafka
    * kafka-clients-0.8.2.1.jar
    * kafka_2.10-0.8.2.1.jar
    * metrics-core-2.2.0.jar
    * zkclient-0.3.jar
* Zookeeper
    * zookeeper-3.4.5.jar

Note: when using the Kafka Data Store with GeoServer in Tomcat it will most likely be necessary to increase the memory settings for Tomcat, `export CATALINA_OPTS="-Xms512M -Xmx1024M -XX:PermSize=256m -XX:MaxPermSize=256m"`.

After placing the dependencies in the correct folder, be sure to restart GeoServer for changes to take place.

Upgrading
---------

To upgrade between minor releases of GeoMesa, the versions of all GeoMesa components **must** match. 

This means that the version of the ``geomesa-distributed-runtime`` JAR installed on Accumulo tablet servers
**must** match the version of the ``geomesa-plugin`` JAR installed in the ``WEB-INF/lib`` directory of GeoServer.

