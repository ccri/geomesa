/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Catch-all for storing instances of Geometry as WKB
 */
public class WKBGeometryVector implements GeometryVector<Geometry, NullableVarBinaryVector> {
  private NullableVarBinaryVector vector;
  static private WKBWriter writer = null;
  static private WKBReader reader = null;

  public static final Field field = Field.nullablePrimitive("wkb", ArrowType.Binary.INSTANCE);

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.Binary.INSTANCE, null, metadata);
  }

  public WKBGeometryVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    this.vector = new NullableVarBinaryVector(name, allocator);
  }

//  public WKBGeometryVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
//    this.vector = container.addOrGet(name, createFieldType(metadata), VarBinaryVector);
//  }

  public WKBGeometryVector(NullableVarBinaryVector vector) {
    vector.allocateNewSafe();
    this.vector = vector;
  }

  //@Override
  static public void setGeom(NullableVarBinaryVector vector, int i, Geometry geom) {
    if (geom == null) {
      vector.getMutator().setNull(i);
    } else {
      if (writer == null) {
        writer = new WKBWriter();
      }
      byte[] wkb = writer.write(geom);

      // JNH Start here tomorrow
      NullableVarBinaryVector.Mutator mutator = vector.getMutator();
      mutator.setIndexDefined(i);
      mutator.setValueLengthSafe(i, wkb.length);
      mutator.setSafe(i, wkb, 0, wkb.length);
    }
  }

  //@Override
  static public Geometry getGeom(NullableVarBinaryVector vector, int i) {
    if (vector.getAccessor().isNull(i)) {
      return null;
    } else {
      Geometry geometry = null;
      try {
        if (reader == null) {
          reader = new WKBReader();
        }
        geometry = reader.read(vector.getAccessor().get(i));
      } catch (ParseException exception) {
        throw new RuntimeException(exception);
      }
      return geometry;
    }
  }

  @Override
  public GeometryWriter<Geometry> getWriter() {
    return new WKBGeometryWriter(vector);
  }

  @Override
  public GeometryReader<Geometry> getReader() {
    return new WKBGeometryReader(vector);
  }

  public static class WKBGeometryWriter implements GeometryWriter<Geometry> {
    NullableVarBinaryVector vector;

    public WKBGeometryWriter(NullableVarBinaryVector vector) {
      this.vector = vector;
    }

    @Override
    public void set(int i, Geometry geom) {
      setGeom(vector, i, geom);
    }

    @Override
    public void setValueCount(int count) {
       vector.getMutator().setValueCount(count);
    }
  }

  public static class WKBGeometryReader implements GeometryReader<Geometry> {
    NullableVarBinaryVector vector;
    public NullableVarBinaryVector.Accessor accessor;

    public WKBGeometryReader(NullableVarBinaryVector vector) {
      this.accessor  = vector.getAccessor();
      this.vector = vector;
    }

    @Override
    public Geometry get(int i) {
      return getGeom(vector, i);
    }

    @Override
    public int getValueCount() {
      return vector.getAccessor().getValueCount();
    }

    @Override
    public int getNullCount() {
      return vector.getAccessor().getNullCount();
    }
  }

  @Override
  public NullableVarBinaryVector getVector() {

    return vector;
  }

//  @Override
//  public void setValueCount(int count) {
//    vector.setValueCount(count);
//  }
//
//  @Override
//  public int getValueCount() {
//    return vector.getValueCount();
//  }
//
//  @Override
//  public int getNullCount() {
//    int count = vector.getNullCount();
//    return Math.max(count, 0);
//  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<Geometry, NullableVarBinaryVector> to) {
    WKBGeometryWriter writer = (WKBGeometryWriter) to.getWriter();

    if (vector.getAccessor().isNull(fromIndex)) {
      writer.vector.getMutator().setNull(toIndex);
    } else {
      byte[] wkb = vector.getAccessor().get(fromIndex);
      NullableVarBinaryVector.Mutator mutator = writer.vector.getMutator();

      // JNH: Copied from above
      //vector.allocateNew();

      mutator.setIndexDefined(toIndex);
      mutator.setValueLengthSafe(toIndex, wkb.length);
      mutator.setSafe(toIndex, wkb, 0, wkb.length);

    }
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }
}
