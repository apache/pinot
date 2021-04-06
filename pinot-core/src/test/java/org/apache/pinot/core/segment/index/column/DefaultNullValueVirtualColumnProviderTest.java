/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.index.column;

import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.ConstantValueDoubleDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueFloatDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.core.segment.index.readers.ConstantValueStringDictionary;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DefaultNullValueVirtualColumnProviderTest {

  FieldSpec svStringFieldSpec = new DimensionFieldSpec("svStringColumn", DataType.STRING, true);
  FieldSpec svIntFieldSpec = new DimensionFieldSpec("svIntColumn", DataType.INT, true);
  FieldSpec svLongFieldSpec = new DimensionFieldSpec("svLongColumn", DataType.LONG, true);
  FieldSpec svDoubleFieldSpec = new DimensionFieldSpec("svDoubleColumn", DataType.DOUBLE, true);
  FieldSpec svFloatFieldSpec = new DimensionFieldSpec("svFloatColumn", DataType.FLOAT, true);
  FieldSpec mvStringFieldSpec = new DimensionFieldSpec("mvStringColumn", DataType.STRING, false);
  FieldSpec mvIntFieldSpec = new DimensionFieldSpec("mvIntColumn", DataType.INT, false);
  FieldSpec mvLongFieldSpec = new DimensionFieldSpec("mvLongColumn", DataType.LONG, false);
  FieldSpec mvDoubleFieldSpec = new DimensionFieldSpec("mvDoubleColumn", DataType.DOUBLE, false);
  FieldSpec mvFloatFieldSpec = new DimensionFieldSpec("mvFloatColumn", DataType.FLOAT, false);

  @Test
  public void testBuildMetadata() {
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(svStringFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("svStringColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.STRING).setTotalDocs(1).setSingleValue(true).setDefaultNullValueString("null")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(svIntFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("svIntColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.INT).setTotalDocs(1).setSingleValue(true).setDefaultNullValueString("-2147483648")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(svLongFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("svLongColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.LONG).setTotalDocs(1).setSingleValue(true)
            .setDefaultNullValueString("-9223372036854775808").setCardinality(1).setHasDictionary(true)
            .setHasInvertedIndex(true).setIsSorted(true).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(svDoubleFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("svDoubleColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.DOUBLE).setTotalDocs(1).setSingleValue(true).setDefaultNullValueString("-Infinity")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(svFloatFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("svFloatColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.FLOAT).setTotalDocs(1).setSingleValue(true).setDefaultNullValueString("-Infinity")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(true).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(mvStringFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("mvStringColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.STRING).setTotalDocs(1).setSingleValue(false).setDefaultNullValueString("null")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(false).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(mvIntFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("mvIntColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.INT).setTotalDocs(1).setSingleValue(false).setDefaultNullValueString("-2147483648")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(false).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(mvLongFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("mvLongColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.LONG).setTotalDocs(1).setSingleValue(false)
            .setDefaultNullValueString("-9223372036854775808").setCardinality(1).setHasDictionary(true)
            .setHasInvertedIndex(true).setIsSorted(false).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(mvDoubleFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("mvDoubleColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.DOUBLE).setTotalDocs(1).setSingleValue(false).setDefaultNullValueString("-Infinity")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(false).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));

    virtualColumnContext = new VirtualColumnContext(mvFloatFieldSpec, 1);
    Assert.assertEquals(
        new ColumnMetadata.Builder().setVirtual(true).setColumnName("mvFloatColumn").setFieldType(FieldType.DIMENSION)
            .setDataType(DataType.FLOAT).setTotalDocs(1).setSingleValue(false).setDefaultNullValueString("-Infinity")
            .setCardinality(1).setHasDictionary(true).setHasInvertedIndex(true).setIsSorted(false).build(),
        new DefaultNullValueVirtualColumnProvider().buildMetadata(virtualColumnContext));
  }

  @Test
  public void testBuildDictionary() {
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(svStringFieldSpec, 1);
    Dictionary dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueStringDictionary.class, dictionary.getClass());
    Assert.assertEquals("null", dictionary.getStringValue(0));

    virtualColumnContext = new VirtualColumnContext(svIntFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueIntDictionary.class, dictionary.getClass());
    Assert.assertEquals(Integer.MIN_VALUE, dictionary.getIntValue(0));

    virtualColumnContext = new VirtualColumnContext(svLongFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueLongDictionary.class, dictionary.getClass());
    Assert.assertEquals(Long.MIN_VALUE, dictionary.getLongValue(0));

    virtualColumnContext = new VirtualColumnContext(svDoubleFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueDoubleDictionary.class, dictionary.getClass());
    Assert.assertEquals(Double.NEGATIVE_INFINITY, dictionary.getDoubleValue(0));

    virtualColumnContext = new VirtualColumnContext(svFloatFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueFloatDictionary.class, dictionary.getClass());
    Assert.assertEquals(Float.NEGATIVE_INFINITY, dictionary.getFloatValue(0));

    virtualColumnContext = new VirtualColumnContext(mvStringFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueStringDictionary.class, dictionary.getClass());
    Assert.assertEquals("null", dictionary.getStringValue(0));

    virtualColumnContext = new VirtualColumnContext(mvIntFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueIntDictionary.class, dictionary.getClass());
    Assert.assertEquals(Integer.MIN_VALUE, dictionary.getIntValue(0));

    virtualColumnContext = new VirtualColumnContext(mvLongFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueLongDictionary.class, dictionary.getClass());
    Assert.assertEquals(Long.MIN_VALUE, dictionary.getLongValue(0));

    virtualColumnContext = new VirtualColumnContext(mvDoubleFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueDoubleDictionary.class, dictionary.getClass());
    Assert.assertEquals(Double.NEGATIVE_INFINITY, dictionary.getDoubleValue(0));

    virtualColumnContext = new VirtualColumnContext(mvFloatFieldSpec, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    Assert.assertEquals(ConstantValueFloatDictionary.class, dictionary.getClass());
    Assert.assertEquals(Float.NEGATIVE_INFINITY, dictionary.getFloatValue(0));
  }
}
