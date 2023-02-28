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
package org.apache.pinot.segment.local.segment.index.column;

import org.apache.pinot.segment.local.segment.index.readers.ConstantValueBytesDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueDoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueFloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueIntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueLongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.ConstantValueStringDictionary;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DefaultNullValueVirtualColumnProviderTest {
  private static final FieldSpec SV_INT = new DimensionFieldSpec("svIntColumn", DataType.INT, true);
  private static final FieldSpec SV_LONG = new DimensionFieldSpec("svLongColumn", DataType.LONG, true);
  private static final FieldSpec SV_FLOAT = new DimensionFieldSpec("svFloatColumn", DataType.FLOAT, true);
  private static final FieldSpec SV_DOUBLE = new DimensionFieldSpec("svDoubleColumn", DataType.DOUBLE, true);
  private static final FieldSpec SV_STRING = new DimensionFieldSpec("svStringColumn", DataType.STRING, true);
  private static final FieldSpec SV_STRING_WITH_DEFAULT =
      new DimensionFieldSpec("svStringColumn", DataType.STRING, true, "default");
  private static final FieldSpec SV_BYTES = new DimensionFieldSpec("svBytesColumn", DataType.BYTES, true);
  private static final FieldSpec MV_INT = new DimensionFieldSpec("mvIntColumn", DataType.INT, false);
  private static final FieldSpec MV_LONG = new DimensionFieldSpec("mvLongColumn", DataType.LONG, false);
  private static final FieldSpec MV_FLOAT = new DimensionFieldSpec("mvFloatColumn", DataType.FLOAT, false);
  private static final FieldSpec MV_DOUBLE = new DimensionFieldSpec("mvDoubleColumn", DataType.DOUBLE, false);
  private static final FieldSpec MV_STRING = new DimensionFieldSpec("mvStringColumn", DataType.STRING, false);

  @Test
  public void testBuildMetadata() {
    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_INT, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_INT).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue((int) SV_INT.getDefaultNullValue())
            .setMaxValue((int) SV_INT.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_LONG, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_LONG).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue((long) SV_LONG.getDefaultNullValue())
            .setMaxValue((long) SV_LONG.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_FLOAT, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_FLOAT).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue((float) SV_FLOAT.getDefaultNullValue())
            .setMaxValue((float) SV_FLOAT.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_DOUBLE, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_DOUBLE).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue((double) SV_DOUBLE.getDefaultNullValue())
            .setMaxValue((double) SV_DOUBLE.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_STRING, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_STRING).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue((String) SV_STRING.getDefaultNullValue())
            .setMaxValue((String) SV_STRING.getDefaultNullValue()).build());

    assertEquals(
        new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_STRING_WITH_DEFAULT, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_STRING_WITH_DEFAULT).setTotalDocs(1).setCardinality(1)
            .setSorted(true).setHasDictionary(true).setMinValue("default").setMaxValue("default").build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(SV_BYTES, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(SV_BYTES).setTotalDocs(1).setCardinality(1).setSorted(true)
            .setHasDictionary(true).setMinValue(new ByteArray((byte[]) SV_BYTES.getDefaultNullValue()))
            .setMaxValue(new ByteArray((byte[]) SV_BYTES.getDefaultNullValue())).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(MV_INT, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(MV_INT).setTotalDocs(1).setCardinality(1).setSorted(false)
            .setHasDictionary(true).setMaxNumberOfMultiValues(1).setMinValue((int) MV_INT.getDefaultNullValue())
            .setMaxValue((int) MV_INT.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(MV_LONG, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(MV_LONG).setTotalDocs(1).setCardinality(1).setSorted(false)
            .setHasDictionary(true).setMaxNumberOfMultiValues(1).setMinValue((long) MV_LONG.getDefaultNullValue())
            .setMaxValue((long) MV_LONG.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(MV_FLOAT, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(MV_FLOAT).setTotalDocs(1).setCardinality(1).setSorted(false)
            .setHasDictionary(true).setMaxNumberOfMultiValues(1).setMinValue((float) MV_FLOAT.getDefaultNullValue())
        .setMaxValue((float) MV_FLOAT.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(MV_DOUBLE, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(MV_DOUBLE).setTotalDocs(1).setCardinality(1).setSorted(false)
            .setHasDictionary(true).setMaxNumberOfMultiValues(1).setMinValue((double) MV_DOUBLE.getDefaultNullValue())
            .setMaxValue((double) MV_DOUBLE.getDefaultNullValue()).build());

    assertEquals(new DefaultNullValueVirtualColumnProvider().buildMetadata(new VirtualColumnContext(MV_STRING, 1)),
        new ColumnMetadataImpl.Builder().setFieldSpec(MV_STRING).setTotalDocs(1).setCardinality(1).setSorted(false)
            .setHasDictionary(true).setMaxNumberOfMultiValues(1).setMinValue((String) MV_STRING.getDefaultNullValue())
            .setMaxValue((String) MV_STRING.getDefaultNullValue()).build());
  }

  @Test
  public void testBuildDictionary() {
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(SV_INT, 1);
    Dictionary dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueIntDictionary.class);
    assertEquals(dictionary.getIntValue(0), Integer.MIN_VALUE);

    virtualColumnContext = new VirtualColumnContext(SV_LONG, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueLongDictionary.class);
    assertEquals(dictionary.getLongValue(0), Long.MIN_VALUE);

    virtualColumnContext = new VirtualColumnContext(SV_FLOAT, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueFloatDictionary.class);
    assertEquals(dictionary.getFloatValue(0), Float.NEGATIVE_INFINITY);

    virtualColumnContext = new VirtualColumnContext(SV_DOUBLE, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueDoubleDictionary.class);
    assertEquals(dictionary.getDoubleValue(0), Double.NEGATIVE_INFINITY);

    virtualColumnContext = new VirtualColumnContext(SV_STRING, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueStringDictionary.class);
    assertEquals(dictionary.getStringValue(0), "null");

    virtualColumnContext = new VirtualColumnContext(SV_BYTES, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueBytesDictionary.class);
    assertEquals(dictionary.getBytesValue(0), new byte[0]);

    virtualColumnContext = new VirtualColumnContext(MV_INT, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueIntDictionary.class);
    assertEquals(dictionary.getIntValue(0), Integer.MIN_VALUE);

    virtualColumnContext = new VirtualColumnContext(MV_LONG, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueLongDictionary.class);
    assertEquals(dictionary.getLongValue(0), Long.MIN_VALUE);

    virtualColumnContext = new VirtualColumnContext(MV_FLOAT, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueFloatDictionary.class);
    assertEquals(dictionary.getFloatValue(0), Float.NEGATIVE_INFINITY);

    virtualColumnContext = new VirtualColumnContext(MV_DOUBLE, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueDoubleDictionary.class);
    assertEquals(dictionary.getDoubleValue(0), Double.NEGATIVE_INFINITY);

    virtualColumnContext = new VirtualColumnContext(MV_STRING, 1);
    dictionary = new DefaultNullValueVirtualColumnProvider().buildDictionary(virtualColumnContext);
    assertEquals(dictionary.getClass(), ConstantValueStringDictionary.class);
    assertEquals(dictionary.getStringValue(0), "null");
  }
}
