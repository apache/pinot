/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.extractors;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PlainFieldExtractorTest {
  private static final DataType[] ALL_TYPES =
      {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.STRING};
  // All types have single/multi-value version.
  private static final int NUMBER_OF_TYPES = 2 * ALL_TYPES.length;
  private static final int INDEX_OF_STRING_TYPE = NUMBER_OF_TYPES - 2;
  private static final String TEST_COLUMN = "testColumn";
  private static final Schema[] ALL_TYPE_SCHEMAS = new Schema[NUMBER_OF_TYPES];

  static {
    int i = 0;
    for (DataType dataType : ALL_TYPES) {
      ALL_TYPE_SCHEMAS[i++] = new Schema.SchemaBuilder().setSchemaName("testSchema")
          .addSingleValueDimension(TEST_COLUMN, dataType)
          .build();
      ALL_TYPE_SCHEMAS[i++] = new Schema.SchemaBuilder().setSchemaName("testSchema")
          .addMultiValueDimension(TEST_COLUMN, dataType)
          .build();
    }
  }

  private class AnyClassWithToString {
    @Override
    public String toString() {
      return "AnyClass";
    }
  }

  @Test
  public void simpleTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension("svDimensionInt", DataType.INT)
        .addSingleValueDimension("svDimensionDouble", DataType.DOUBLE)
        .addSingleValueDimension("svClassObject", DataType.STRING)
        .addMultiValueDimension("mvDimensionLong", DataType.LONG)
        .addMultiValueDimension("mvClassObject", DataType.STRING)
        .addMetric("metricInt", DataType.INT)
        .addTime("timeInt", TimeUnit.DAYS, DataType.INT)
        .build();
    PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put("svDimensionInt", (short) 5);
    fieldMap.put("svDimensionDouble", 3.2F);
    fieldMap.put("svClassObject", new AnyClassWithToString());
    fieldMap.put("mvDimensionLong", 13);
    fieldMap.put("mvClassObject", new Object[]{new AnyClassWithToString(), new AnyClassWithToString()});
    fieldMap.put("metricInt", 34.5);
    long currentDaysSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24;
    fieldMap.put("timeInt", currentDaysSinceEpoch);

    row.init(fieldMap);
    row = plainFieldExtractor.transform(row);

    Assert.assertTrue(row.getValue("svDimensionInt") instanceof Integer);
    Assert.assertEquals(row.getValue("svDimensionInt"), 5);
    Assert.assertTrue(row.getValue("svDimensionDouble") instanceof Double);
    Assert.assertEquals((double) row.getValue("svDimensionDouble"), 3.2, 0.1);
    Assert.assertTrue(row.getValue("svClassObject") instanceof String);
    Assert.assertEquals(row.getValue("svClassObject"), "AnyClass");
    Assert.assertTrue(row.getValue("mvDimensionLong") instanceof Object[]);
    Assert.assertTrue(((Object[]) row.getValue("mvDimensionLong"))[0] instanceof Long);
    Assert.assertEquals(((Object[]) row.getValue("mvDimensionLong"))[0], 13L);
    Assert.assertTrue(row.getValue("mvClassObject") instanceof Object[]);
    Assert.assertTrue(((Object[]) row.getValue("mvClassObject"))[0] instanceof String);
    Assert.assertTrue(((Object[]) row.getValue("mvClassObject"))[1] instanceof String);
    Assert.assertEquals(((Object[]) row.getValue("mvClassObject"))[0], "AnyClass");
    Assert.assertEquals(((Object[]) row.getValue("mvClassObject"))[1], "AnyClass");
    Assert.assertTrue(row.getValue("metricInt") instanceof Integer);
    Assert.assertEquals(row.getValue("metricInt"), 34);
    Assert.assertTrue(row.getValue("timeInt") instanceof Integer);
    Assert.assertEquals(row.getValue("timeInt"), (int) currentDaysSinceEpoch);
  }

  @Test
  public void nullValueTest() {
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    for (int i = 0; i < NUMBER_OF_TYPES; i++) {
      PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(ALL_TYPE_SCHEMAS[i]);
      row.init(fieldMap);
      plainFieldExtractor.transform(row);

      Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 0);
      Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 1);
      Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 0);
    }
  }

  @Test
  public void classWithToStringTest() {
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    for (int i = 0; i < NUMBER_OF_TYPES; i++) {
      PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(ALL_TYPE_SCHEMAS[i]);
      fieldMap.put(TEST_COLUMN, new AnyClassWithToString());
      row.init(fieldMap);
      plainFieldExtractor.transform(row);
      fieldMap.put(TEST_COLUMN, new Object[]{new AnyClassWithToString(), new AnyClassWithToString()});
      row.init(fieldMap);
      plainFieldExtractor.transform(row);

      // AnyClassWithToString only works with String (array).
      if (i >= INDEX_OF_STRING_TYPE) {
        Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 0);
        Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
        Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 2);
      } else {
        Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 2);
        Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
        Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 0);
      }
    }
  }

  @Test
  public void automatedTest() {
    int numTypes = 19;
    Object[] objectArray = new Object[numTypes];

    // Pinot data types.
    objectArray[0] = 500;                                       // Integer
    objectArray[1] = new Object[]{500};                         // Integer array
    objectArray[2] = 500L;                                      // Long
    objectArray[3] = new Object[]{500L};                        // Long array
    objectArray[4] = 500.5F;                                    // Float
    objectArray[5] = new Object[]{500.5F};                      // Float array
    objectArray[6] = 500.5;                                     // Double
    objectArray[7] = new Object[]{500.5};                       // Double array
    objectArray[8] = "500";                                     // String
    objectArray[9] = new Object[]{"500"};                       // String array

    // Non-Pinot data types.
    objectArray[10] = true;                                     // Boolean
    objectArray[11] = (byte) 65;                                // Byte
    objectArray[12] = new Object[]{(byte) 65};                  // Byte array
    objectArray[13] = 'a';                                      // Character
    objectArray[14] = new Object[]{'a'};                        // Character array
    objectArray[15] = (short) 500;                              // Short
    objectArray[16] = new Object[]{(short) 500};                // Short array
    objectArray[17] = new AnyClassWithToString();               // Object
    objectArray[18] = new Object[]{new AnyClassWithToString()}; // Object array

    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    for (int i = 0; i < NUMBER_OF_TYPES; i++) {
      for (int j = 0; j < numTypes; j++) {
        PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(ALL_TYPE_SCHEMAS[i]);
        fieldMap.put(TEST_COLUMN, objectArray[j]);
        row.init(fieldMap);
        plainFieldExtractor.transform(row);

        // Check when schema and field match.
        if (i == j) {
          Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 0);
          Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
          Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 0);
          continue;
        }

        // Check conversions from Object (array).
        if (j == 10 || j >= 17/* Index of AnyClassWithToString */) {
          if (i >= INDEX_OF_STRING_TYPE) {
            // Conversions from Boolean or Object (array) to String (array). (Allowed)
            Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 0);
            Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
            Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 1);
          } else {
            // Conversions from Boolean or Object (array) to non-String (array). (Not allowed)
            Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 1);
            Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
            Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 0);
          }
          continue;
        }

        // Check other conversions. (Because string value is "500", it can convert to any type)
        Assert.assertEquals(plainFieldExtractor.getTotalErrors(), 0);
        Assert.assertEquals(plainFieldExtractor.getTotalNulls(), 0);
        Assert.assertEquals(plainFieldExtractor.getTotalConversions(), 1);
      }
    }
  }

  @Test
  public void timeSpecStringTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime("timeString", TimeUnit.DAYS, DataType.STRING)
        .build();
    PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    fieldMap.put("timeString", "2016-01-01");
    row.init(fieldMap);
    plainFieldExtractor.transform(row);

    Assert.assertTrue(row.getValue("timeString") instanceof String);
    Assert.assertEquals(row.getValue("timeString"), "2016-01-01");
  }

  @Test
  public void differentIncomingOutgoingTimeSpecTest() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addTime("incoming", TimeUnit.DAYS, DataType.INT, "outgoing", TimeUnit.HOURS, DataType.LONG)
        .build();
    PlainFieldExtractor plainFieldExtractor = new PlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<>();
    long currentDaysSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24;
    fieldMap.put("incoming", currentDaysSinceEpoch);
    row.init(fieldMap);
    row = plainFieldExtractor.transform(row);

    Assert.assertNull(row.getValue("incoming"));
    Assert.assertTrue(row.getValue("outgoing") instanceof Long);
    Assert.assertEquals(row.getValue("outgoing"), currentDaysSinceEpoch * 24);
  }
}
