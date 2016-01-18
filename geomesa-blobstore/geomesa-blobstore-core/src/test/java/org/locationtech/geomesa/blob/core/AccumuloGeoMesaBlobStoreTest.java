/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *************************************************************************/

package org.locationtech.geomesa.blob.core;

import org.junit.Before;
import org.junit.Test;
import org.locationtech.geomesa.blob.core.impl.AccumuloGeoMesaBlobStore;
import scala.Option;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccumuloGeoMesaBlobStoreTest {


    AccumuloGeoMesaBlobStore agbs;

    @Before
    public void before() {
        Map<String, Serializable> testParams = new HashMap<>();
        testParams.put("instanceId", "mycloud");
        testParams.put("zookeepers", "zoo1:2181,zoo2:2181,zoo3:2181");
        testParams.put("user", "myuser");
        testParams.put("password", "mypassword");
        testParams.put("tableName", "geomesaJava");
        testParams.put("useMock", "true");
        try {
            agbs = new AccumuloGeoMesaBlobStore(testParams);
        } catch(Exception e) {
            System.out.println("Error initializing test geomesa blob store in AccumuloGeoMesaBlobStoreTest.java");
        }
    }


    @Test
    public void testBlobStoreIngestAndQuery() {
        File test1 = new File(getClass().getClassLoader().getResource("testFile.txt").getFile());
        Map<String, String> wkt = new HashMap<>();
        wkt.put("wkt", "POINT (0 0)");

        Option<String> id = agbs.put(test1, wkt);
        assertTrue(id.isDefined());

        Tuple2<byte[], String> result = agbs.get(id.get());
        assertEquals(result._2, "testFile.txt");
    }

}
