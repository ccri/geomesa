GeoMesa BlobStore EXIF
======================

This tutorial will show you:

1. How to deploy the EXIF handler to ingest geotagged images
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
for the handler into the existing classpath. For this tutorial, we assume the user is deploying
the EXIF FileHandler to an existing GeoServer with the BlobStore web servlet. See instructions above for
setting up the BlobStore in the prerequisites.

To deploy the EXIF FileHandler run:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/handlers/geomesa-blobstore-exif-handler-$VERSION.jar \
      -C /path/to/tomcat/webapps/geoserver/WEB-INF/lib/

If you are using GeoServer's built in Jetty web server:

.. code-block:: bash

    $ tar -xzvf \
      geomesa-$VERSION/dist/handlers/geomesa-blobstore-exif-handler-$VERSION.jar\
      -C /path/to/geoserver/webapps/geoserver/WEB-INF/lib/

Now the BlobStore is able to read image files with EXIF metadata headers.


Using the EXIF Handler
----------------------
In order to use our new handler, we will need to get some geotagged images.
Below is a partial Python script that uses the Flickr API to grab some geotagged images taken around Monument Valley.
Using the Flickr API is beyond the scope of this tutorial, however the Python code snippet is provided for those
interested in obtaining a test data set. The below script will download 25 images to the home user directory.

.. code-block:: python

    import flickrapi
    import requests
    api_key = u'Some API Key'
    secret  = u'some secret'

    flickr = flickrapi.FlickrAPI(api_key, secret)
    flickr.authenticate_console(perms='read')
    photos = flickr.photos_search(lat='37.0000', lon='-110.1700', radius='10', safe_search='1', extras='url_o')

    i = 0
    for photo in photos[0][0:25]:
        if 'url_o' in photo.keys():
            url = photo.attrib['url_o']
            with open('~/flickr{}.jpg'.format(i), 'wb') as fd:
                r = requests.get(url)
                for chunk in r.iter_content(1024):
                    fd.write(chunk)
                i += 1


First we need to register a BlobStore using the following command:

.. code-block:: bash

    $ curl -d 'instanceId=myCloud' -d 'zookeepers=zoo1,zoo2,zoo3' -d 'tableName=myblobstore' -d 'user=user' -d 'password=password' http://localhost:8080/geoserver/geomesa/blobstore/ds/myblobstore

To ingest the files, we can write a simple bash loop to use cURL on each file to ingest to the BlobStore.

.. code-block:: bash

    $ for f in *.jpg; do curl -X POST -F file=@$f http://localhost:8080/geoserver/geomesa/blobstore/blob/myblobstore ; done

Files have now been ingested.


Register Index table in GeoServer
---------------------------------

The BlobStore index tables are ordinary GeoTools Data Stores, so the registration in GeoServer is no different.


Querying the Index for Blobs
----------------------------


Downloading Blobs
-----------------



