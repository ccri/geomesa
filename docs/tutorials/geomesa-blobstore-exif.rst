GeoMesa BlobStore EXIF
======================

This tutorial will show you:

1. How to use the EXIF handler to ingest geotagged images
2. How to view the spatial index of the BlobStore in geoserver
3. How to query the index for individual blobs
4. How to download blobs from the BlobStore

Prerequisites
-------------

.. warning::

    For Accumulo deployment, you will need access to an Accumulo |accumulo_version| instance.

Before you begin, you should have these:

-  basic knowledge of `GeoTools <http://www.geotools.org>`__ and
   `GeoServer <http://geoserver.org>`__
-  access to a `GeoServer <http://geoserver.org/>`__ 2.8.x installation
-  an Accumulo user that has both create-table and write permissions

Before you begin, you should have followed the instructions for setting up the blobstore in :doc:`/user/blobstore`.

Introduction
------------

The GeoMesa BlobStore is able to handle a variety of file types by using a pluggable interface for FileHandlers.
This allows support for new file formats to be quickly integrated. GeoMesa comes with a handler for EXIF headers,
which are included in a variety of image formats from digital cameras that will be demonstrated by this tutorial.

GeoMesa BlobStore EXIF Handler Installation Instructions
--------------------------------------------------------

As the FileHandlers use the Java Service Registry, they can be loaded at runtime by simply placing the jar
for the handler into the existing classpath.

