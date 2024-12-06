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
package org.apache.pinot.plugin.minion.tasks.mergerollup;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.recordtransformer.RecordTransformer;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class DimensionValueTransformerTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("svInt", DataType.INT)
      .addSingleValueDimension("svLong", DataType.LONG).addSingleValueDimension("svFloat", DataType.FLOAT)
      .addSingleValueDimension("svDouble", DataType.DOUBLE).addSingleValueDimension("svBoolean", DataType.BOOLEAN)
      .addSingleValueDimension("svTimestamp", DataType.TIMESTAMP).addSingleValueDimension("svBytes", DataType.BYTES)
      .addMultiValueDimension("mvInt", DataType.INT).addSingleValueDimension("svJson", DataType.JSON)
      .addMultiValueDimension("mvLong", DataType.LONG).addMultiValueDimension("mvFloat", DataType.FLOAT)
      .addMultiValueDimension("mvDouble", DataType.DOUBLE)
      .addSingleValueDimension("svStringWithNullCharacters", DataType.STRING)
      .addSingleValueDimension("svStringWithLengthLimit", DataType.STRING)
      .addMultiValueDimension("mvString1", DataType.STRING).build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  static {
    SCHEMA.getFieldSpecFor("svStringWithLengthLimit").setMaxLength(2);
    SCHEMA.addField(new DimensionFieldSpec("$virtual", DataType.STRING, true, Object.class));
  }

  // Transform multiple times should return the same result
  private static final int NUM_ROUNDS = 5;

  private static GenericRow getRecord() {
    GenericRow record = new GenericRow();
    record.putValue("svInt", (byte) 123);
    record.putValue("svLong", (char) 123);
    record.putValue("svFloat", Collections.singletonList((short) 123));
    record.putValue("svDouble", new String[]{"123"});
    record.putValue("svBoolean", "true");
    record.putValue("svTimestamp", "2020-02-02 22:22:22.222");
    record.putValue("svBytes", "7b7b"/*new byte[]{123, 123}*/);
    record.putValue("svJson", "{\"first\": \"daffy\", \"last\": \"duck\"}");
    record.putValue("mvInt", new Object[]{123L});
    record.putValue("mvLong", Collections.singletonList(123f));
    record.putValue("mvFloat", new Double[]{123d});
    record.putValue("mvDouble", Collections.singletonMap("key", 123));
    record.putValue("mvString1", new Object[]{"123", 123, 123L, 123f, 123.0});
    record.putValue("svFloatNegativeZero", -0.00f);
    return record;
  }

  @Test
  public void testDimensionValueTransformer() {
    Map<String, String> taskConfig = new HashMap<>();
    taskConfig.put(MinionConstants.MergeRollupTask.ERASE_DIMENSION_VALUES_KEY,
        "svInt, svLong, svFloat, svDouble, svBoolean, svTimestamp, svJson, svBytes, mvInt, mvLong, mvFloat, mvDouble,"
            + " mvString1, $virtual");
    Set<String> dimensionsToErase = MergeRollupTaskUtils.getDimensionsToErase(taskConfig);
    RecordTransformer transformer = new DimensionValueTransformer(SCHEMA, dimensionsToErase);

    GenericRow record = getRecord();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(record.getValue("svLong"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
      assertEquals(record.getValue("svFloat"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
      assertEquals(record.getValue("svDouble"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
      assertEquals(record.getValue("svBoolean"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN);
      assertEquals(record.getValue("svTimestamp"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
      assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
      assertEquals(record.getValue("svJson"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
      assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
      assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
      assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
      assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
      assertEquals(record.getValue("mvString1"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    }

    // Test empty record
    record = new GenericRow();
    for (int i = 0; i < NUM_ROUNDS; i++) {
      record = transformer.transform(record);
      assertNotNull(record);
      assertEquals(record.getValue("svInt"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
      assertEquals(record.getValue("svLong"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
      assertEquals(record.getValue("svFloat"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
      assertEquals(record.getValue("svDouble"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
      assertEquals(record.getValue("svBoolean"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN);
      assertEquals(record.getValue("svTimestamp"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP);
      assertEquals(record.getValue("svBytes"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
      assertEquals(record.getValue("svJson"), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON);
      assertEquals(record.getValue("mvInt"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
      assertEquals(record.getValue("mvLong"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG});
      assertEquals(record.getValue("mvFloat"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT});
      assertEquals(record.getValue("mvDouble"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE});
      assertEquals(record.getValue("mvString1"), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING});
    }
  }
}
