Architecture Overview
=====================

For scalability, GeoMesa is built on technologies that can use Apache Hadoop. This includes data stores such as Accumulo, HBase, and Google Cloud Bigtable, as well as the Apache Kafka message broker for streaming data. Apache Storm lets you define information sources and manipulations to allow batch, distributed processing of streaming data with GeoMesa, and a GeoMesa environment can also take advantage of Apache Spark to do large-scale analytics of stored and streaming data.

|gmhadoopinfrastructure|


Integration
-----------

To expose the geospatial data that it stores for use by your applications,
GeoMesa implements `GeoTools <http://geotools.org/>`_ interfaces to provide HTTP access to the following Open Geospatial Consortium standards:

* `Web Feature Service (WFS) <http://www.opengeospatial.org/standards/wfs>`_
* `Web Mapping Service (WMS) <http://www.opengeospatial.org/standards/wms>`_
* `Web Processing Service (WPS) <http://www.opengeospatial.org/standards/wps>`_
* `Web Coverage Service (WCS) <http://www.opengeospatial.org/standards/wcs>`_

If an application already uses GeoServer, integration with GeoMesa is simply a matter of adding a new datastore to GeoServer and updating the application’s configuration.

Geohashing and Z-curves
-----------------------

TODO

Data Structure
--------------

TODO - or maybe under developing?

.. |locationtech-icon| image:: /_static/img/locationtech.png
.. |gmhadoopinfrastructure| image:: /_static/img/gmhadoopinfrastructure.png
