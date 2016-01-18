/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.blob.core.impl;

import com.vividsolutions.jts.geom.Geometry;
import org.geotools.data.Query;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory;
import org.locationtech.geomesa.blob.core.AccumuloBlobStore;
import org.locationtech.geomesa.blob.core.interop.GeoMesaBlobStore;
import org.opengis.filter.Filter;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AccumuloGeoMesaBlobStore implements GeoMesaBlobStore {

    private AccumuloDataStoreFactory accumuloDataStoreFactory = new AccumuloDataStoreFactory();
    public AccumuloBlobStore accumuloBlobStore;

    public AccumuloGeoMesaBlobStore(Map<String, Serializable> dataStoreParams) throws Exception {
        AccumuloDataStore ds = (AccumuloDataStore) accumuloDataStoreFactory.createDataStore(dataStoreParams);
        if (ds == null) {
            throw new Exception("Error initializing AccumuloGeoMesaBlobStore");
        } else {
            accumuloBlobStore = new AccumuloBlobStore(ds);
        }
    }

    public AccumuloGeoMesaBlobStore(String instanceId, String tableName, String zookeepers, String user, String password) throws Exception {
        Map<String, Serializable> dataStoreParams = new HashMap<>();
        dataStoreParams.put("instanceId", instanceId);
        dataStoreParams.put("tableName", tableName);
        dataStoreParams.put("zookeepers", zookeepers);
        dataStoreParams.put("user", user);
        dataStoreParams.put("password", password);
        new AccumuloGeoMesaBlobStore(dataStoreParams);
    }


    /**
     * Add a File to the blobstore, relying on available FileHandlers to determine ingest
     *  @param file   File to ingest
     * @param params Map String to String, see AccumuloBlobStore for keys
     */
    @Override
    public Option<String> put(File file, Map<String, String> params) {
        return accumuloBlobStore.put(file, params);
    }

    /**
     * @param fis    FileInputStream to ingest, bypass FileHandlers to rely on client to set params
     * @param params Map String to String, see AccumuloBlobStore for keys
     */
    @Override
    public String put(FileInputStream fis, Map<String, String> params) {
        return accumuloBlobStore.put(fis, params);
    }

    /**
     * @param fis      FileInputStream to ingest, bypass FileHandlers to rely on other function params
     * @param filename Filename corresponding to the FileInputStream, including extension
     * @param geometry Geometry representing the FileInputStream, must be in EPSG:4326
     * @param dtg      Date corrisponding to the FileInputStream, can be null
     */
    @Override
    public String put(FileInputStream fis, String filename, Geometry geometry, Date dtg) {
        return accumuloBlobStore.put(fis, filename, geometry, dtg);
    }

    /**
     * Query BlobStore for Ids by a opengis Filter
     *
     * @param filter Filter used to query blobstore by
     * @return Iterator of blob Ids that can then be downloaded via get
     */
    @Override
    public Iterator<String> getIds(Filter filter) {
        return accumuloBlobStore.getIds(filter);
    }

    /**
     * Query BlobStore for Ids by a GeoTools Query
     *
     * @param query Query used to query blobstore by
     * @return
     */
    @Override
    public Iterator<String> getIds(Query query) {
        return accumuloBlobStore.getIds(query);
    }

    /**
     * Fetches Blob by id
     *
     * @param id String feature Id of the Blob, from getIds functions
     * @return Tuple2 of (blob, filename)
     */
    @Override
    public Tuple2<byte[], String> get(String id) {
        return accumuloBlobStore.get(id);
    }
}
