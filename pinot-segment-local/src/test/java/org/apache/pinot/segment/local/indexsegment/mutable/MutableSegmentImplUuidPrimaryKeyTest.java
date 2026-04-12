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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Regression tests for UUID primary-key normalization during realtime upsert ingestion.
 */
public class MutableSegmentImplUuidPrimaryKeyTest {
  private static final String UUID_PK_COLUMN = "uuidPk";
  private static final String VALUE_COLUMN = "payload";
  private static final String TIME_COLUMN = "ts";
  private static final String UUID_VALUE = "550e8400-e29b-41d4-a716-446655440000";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(UUID_PK_COLUMN, FieldSpec.DataType.UUID)
      .addSingleValueDimension(VALUE_COLUMN, FieldSpec.DataType.STRING)
      .addDateTimeField(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
      .setPrimaryKeyColumns(List.of(UUID_PK_COLUMN))
      .build();

  private MutableSegmentImpl _segment;

  @AfterMethod
  public void tearDown() {
    if (_segment != null) {
      _segment.destroy();
      _segment = null;
    }
  }

  @Test
  public void testUpsertPrimaryKeyNormalizesMixedCaseUuidStrings()
      throws ReflectiveOperationException {
    _segment = MutableSegmentImplTestUtils.createMutableSegmentImpl(SCHEMA, false, TIME_COLUMN, null, null);

    PrimaryKey firstPrimaryKey = getPrimaryKey(createRow(UUID_VALUE.toUpperCase(), 1L));
    PrimaryKey secondPrimaryKey = getPrimaryKey(createRow(UUID_VALUE, 2L));
    assertEquals(firstPrimaryKey, secondPrimaryKey);
    assertTrue(firstPrimaryKey.getValues()[0] instanceof ByteArray);
    assertEquals(firstPrimaryKey.getValues()[0], new ByteArray(UuidUtils.toBytes(UUID_VALUE)));
  }

  private GenericRow createRow(String uuidValue, long timestamp) {
    GenericRow row = new GenericRow();
    row.putValue(UUID_PK_COLUMN, uuidValue);
    row.putValue(VALUE_COLUMN, "payload-" + timestamp);
    row.putValue(TIME_COLUMN, timestamp);
    return row;
  }

  private PrimaryKey getPrimaryKey(GenericRow row)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method getPrimaryKeyMethod = MutableSegmentImpl.class.getDeclaredMethod("getPrimaryKey", GenericRow.class);
    getPrimaryKeyMethod.setAccessible(true);
    return (PrimaryKey) getPrimaryKeyMethod.invoke(_segment, row);
  }
}
