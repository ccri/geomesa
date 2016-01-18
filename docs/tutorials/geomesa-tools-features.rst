Management with GeoMesa Tools
=============================

GeoMesa Tools is a set of command line tools to add feature management
functions, query planning and explanation, ingest, and export abilities
from the command line. In this tutorial, we'll cover initial setup of
GeoMesa Tools and the feature management commands included with the
module.

Configuration
-------------

To begin using the command line tools, first follow the `GeoMesa
Deployment Tutorial </geomesa-deployment/>`__ to download and install
the full GeoMesa project.

This tutorial runs through the beginning steps of testing the GeoMesa
Command Line Tools, and some of those tests are repeated in this
tutorial. After following the `GeoMesa Deployment
Tutorial </geomesa-deployment/>`__, you should be able to use GeoMesa
from any directory on your computer. To test, ``cd`` to a different
directory and run:

.. code:: bash

    $ geomesa

This should print out the following usage text, where the GEOMESA_HOME value shows where GeoMesa was installed:

.. code:: bash

    Using GEOMESA_HOME = /home/jkenneally/geomesa-$VERSION
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

If you get an error message related to the SLF4J library, please refer
to the `Deployment Tutorial </geomesa-deployment/>`__ for more
information.

This usage text gives a brief overview of how to use each command. To
learn more about each command the the flags associated with them, take a
look at the `GeoMesa Tools
README <https://github.com/locationtech/geomesa/blob/master/geomesa-tools/README.md>`__.

Throughout this tutorial, be aware that all commands will require ``-u``
or ``--user`` flags, where the username is your Accumulo username. The
password for the Accumulo user can also be given on the command line
with ``-p`` or ``--password``, or GeoMesa Tools can prompt you for your
password after entering a command, ensuring your password won't enter
shell history.

If you run into issues with Hadoop JARs not being found, try appending
your commands with

.. code:: bash

    --instance-name <accumulo-instance-name> --zookeepers <accumulo-zookeepers>

where ``<accumulo-instance-name>`` and ``<accumulo-zookeepers>`` are
specific to your Accumulo setup. This will override GeoMesa Tools
attempting to automatically find your Accumulo and Hadoop
configurations.

Additionally, if you need visibilities or authorizations on your
Accumulo tables, every command accepts ``--visibilities`` and
``--auths`` for Accumulo connection parameters.

Feature Management
------------------

create: creating a feature type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To begin, let's start by creating a new feature type in GeoMesa with the
``create`` command. The ``create`` command takes three required and one
optional flag:

**Required**

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-fn`` or ``--feature-name``: the name of the feature
-  ``-s`` or ``--spec``: the ``SimpleFeatureType`` specification

**Optional**

-  ``-dt`` or ``--dt-field``: the default date attribute of the
   ``SimpleFeatureType``

Run the command:

.. code:: bash

    $ geomesa create -u <username> -p <password> \
    -c cmd_tutorial \
    -fn feature \
    -s id:String:index=true,dtg:Date,geom:Point:srid=4326 \
    -dt dtg

This will create a new feature type, named "feature", on the GeoMesa
catalog table "cmd\_tutorial". The catalog table stores metadata
information about each feature, and it will be used to prefix each table
name in Accumulo.

If the above command was successful, you should see output similar to
the following:

.. code:: bash

    Creating 'cmd_tutorial_feature' with spec 'id:String:index=true,dtg:Date,geom:Point:srid=4326'. Just a few moments...
    Feature 'cmd_tutorial_feature' with spec 'id:String:index=true,dtg:Date,geom:Point:srid=4326' successfully created.

Now that you've seen how to create feature types, create another feature
type on catalog table "cmd\_tutorial" using your own first name for the
``--feature-name`` and the above schema for the ``--spec``.

list: listing known feature types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should have two feature types on catalog table "cmd\_tutorial". To
verify, we'll use the ``list`` command. The ``list`` command takes one
flag:

-  ``-c`` or ``--catalog``: the name of the catalog table

Run the following command:

.. code:: bash

    $ geomesa list -u <username> -p <password> -c cmd_tutorial

The output text should be something like:

.. code:: bash

    Listing features on 'cmd_tutorial'. Just a few moments...
    2 features exist on 'cmd_tutorial'. They are:
    feature
    jake

describe: finding the attributes of a feature type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To find out more about the attributes of a feature type, we'll use the
``describe`` command. This command takes two flags:

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-fn`` or ``--feature-name``: the name of the feature type

Let's find out more about the attributes on our first feature type. Run
the command

.. code:: bash

    $ geomesa describe -u <username> -p <password> -c cmd_tutorial -fn feature

The output should look like:

.. code:: bash

    Describing attributes of feature 'cmd_tutorial_feature'. Just a few moments...
    id: String (Indexed)
    dtg: Date (Time-index)
    geom: Point (Geo-index)

removeschema: deleting a feature type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Continuing on, let's delete the first feature type we created with the
``removeschema`` command. The ``removeschema`` command takes two flags:

-  ``-c`` or ``--catalog``: the name of the catalog table
-  ``-fn`` or ``--feature-name``: the name of the feature to delete

Run the following command:

.. code:: bash

    geomesa removeschema -u <username> -p <password> -c cmd_tutorial -fn feature

NOTE: Running this command will take a bit longer than the previous two,
as it will delete three tables in Accumulo, as well as remove the
metadata rows in the catalog table associated with the feature.

The output should resemble the following:

.. code:: bash

    Remove schema feature from catalog cmd_tutorial? (yes/no): yes
    Starting
    State change: CONNECTED
    Removed feature

Conclusion
----------

In this tutorial, you learned about the feature management commands
``create``, ``list``, ``describe``, and ``removeschema`` in the GeoMesa
Tools module. If you have ideas for additional functionality to include
in the GeoMesa Tools module, please don't hesitate to `reach out on our
listserv <mailto:geomesa-users@locationtech.org>`__.
