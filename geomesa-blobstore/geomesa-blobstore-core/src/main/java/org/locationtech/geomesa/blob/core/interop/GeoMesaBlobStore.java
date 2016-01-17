/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.blob.core.interop;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.Query;
import org.opengis.filter.Filter;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * An interface to define how users may ingest and query a GeoMesa BlobStore
 */
public interface GeoMesaBlobStore {

    /**
     * Add a File to the blobstore, relying on available FileHandlers to determine ingest
     * @param file File to ingest
     * @param params Map String to String, see AccumuloBlobStore for keys
     */
    public Option<String> put(File file, Map<String, String> params);

    /**
     *
     * @param fis FileInputStream to ingest, bypass FileHandlers to rely on client to set params
     * @param params Map String to String, see AccumuloBlobStore for keys
     */
    public String put(FileInputStream fis, Map<String, String> params);

    /**
     *
     * @param fis FileInputStream to ingest, bypass FileHandlers to rely on other function params
     * @param filename Filename corresponding to the FileInputStream, including extension
     * @param geometry Geometry representing the FileInputStream, must be in EPSG:4326
     * @param dtg Date corrisponding to the FileInputStream, can be null
     */
    public String put(FileInputStream fis, String filename, Geometry geometry, Date dtg);

    /**
     * Query BlobStore for Ids by a opengis Filter
     * @param filter Filter used to query blobstore by
     * @return Iterator of blob Ids that can then be downloaded via get
     */
    public Iterator<String> getIds(Filter filter);

    /**
     * Query BlobStore for Ids by a GeoTools Query
     * @param query Query used to query blobstore by
     * @return
     */
    public Iterator<String> getIds(Query query);

    /**
     * Fetches Blob by id
     * @param id String feature Id of the Blob, from getIds functions
     * @return Tuple2 of (blob, filename)
     */
    public Tuple2<byte[], String> get(String id);

}
