GeoMesa GeoServer UI
====================

BACKGROUND
----------

When you install the GeoMesa plugin for GeoServer, you get access to the
GeoMesa User Interface. The GeoMesa UI shows the status of your GeoMesa
data stores and will eventually provide additional features such as the
abillity to build indices on your data and calculate statistics on your common
queries.

This section outlines some of the features currently available through the
GeoMesa UI.

Instructions for installing the GeoMesa plugin in GeoServer are
available `here </geomesa-deployment/>`__, under 'Deploy GeoMesa
Accumulo Plugin to GeoServer'.

ACCESS TO THE USER INTERFACE
----------------------------

You can reach the GeoServer main screen by sending a browser to ``http://127.0.0.1:8080/geoserver`` and logging in with a username of ``admin`` and a password of  ``geoserver``. (This URL and password are default values and may be different at your own installation.) 

Once you have installed the GeoMesa plugin, the GeoServer administration interface will include a GeoMesa menu on the sidebar:

.. figure:: _static/img/tutorials/2014-08-06-geomesa-ui/geoserver-menu.png
   :alt: "GeoMesa Menu"

DATA STORE SUMMARY
------------------

Any GeoMesa data stores that you have added to GeoServer can be examined
on the Data Stores page. The top of that page has a table listing
all of your GeoMesa data stores. Underneath that are two charts.
The first shows the number of records in your different
features, and the second displays the progress of any ingestion currently in progress.

.. note::

    In order for the ingest chart to display, the Accumulo monitor must be running and the
    configuration page must have the correct address for the monitor (see below).

Further down the page, GeoServer displays statistics on each feature. This
shows the different tables used to store the feature in Accumulo, the number
of tablets per table, and the total number of entries. Clicking the 'Feature
Attributes' link displays a list of all the attributes for the feature shows
whether they are indexed for querying.

.. figure:: _static/img/tutorials/2014-08-06-geomesa-ui/geoserver-datastores.png
   :alt: "Hadoop Status"

CONFIGURATION
-------------

To use certain UI features you'll need to first set the appropriate
properties on the configuration page. Most of these properties
correspond to Hadoop properties, and they can be copied from your hadoop
configuration files. You can enter them by hand, or you can upload your
hadoop configuration files directly to the page. To do this, use the
'Load from XML' button.

.. figure:: _static/img/tutorials/2014-08-06-geomesa-ui/geoserver-config.png
   :alt: "GeoMesa Configuration"

HADOOP STATUS
-------------

Once the configuration is done, you can monitor the Hadoop cluster
status on the Hadoop Status page. Here you can see the load on your
cluster and any currently running jobs.

.. figure:: _static/img/tutorials/2014-08-06-geomesa-ui/geoserver-hadoop-status.png
   :alt: "Hadoop Status"

