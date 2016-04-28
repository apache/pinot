/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.Schema.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainFieldExtractorTest {
  @Test
  public void simpleTest()
      throws Exception {
    Schema schema = new SchemaBuilder().addSingleValueDimension("svDimensionInt", DataType.INT)
        .addSingleValueDimension("svDimensionDouble", DataType.DOUBLE)
        .addMultiValueDimension("mvDimension", DataType.STRING, ",")
        .addMetric("metric", DataType.INT)
        .addTime("incomingTime", TimeUnit.DAYS, DataType.LONG)
        .build();
    PlainFieldExtractor extractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    Short shortObj = new Short((short) 5);
    fieldMap.put("svDimensionInt", shortObj);
    Float floatObj = new Float((float) 3.2);
    fieldMap.put("svDimensionDouble", floatObj);
    Double doubleObj = new Double((double) 34.5);
    fieldMap.put("metric", doubleObj);
    long currentDaysSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24;
    fieldMap.put("incomingTime", currentDaysSinceEpoch);
    row.init(fieldMap);
    extractor.transform(row);
    Assert.assertTrue(row.getValue("svDimensionInt") instanceof Integer);
    Assert.assertTrue(row.getValue("svDimensionDouble") instanceof Double);
    Assert.assertTrue(row.getValue("mvDimension") != null);
    Assert.assertTrue(row.getValue("metric") instanceof Integer);
    Assert.assertTrue((Integer) row.getValue("metric") == 34);
    Assert.assertTrue((Long) row.getValue("incomingTime") == currentDaysSinceEpoch);
  }

  @Test
  public void automatedTest()
      throws Exception {
    final Logger LOGGER = LoggerFactory.getLogger(PlainFieldExtractorTest.class);
    final int numberOfTypes = 18;
    final String columnName = "testColumn";
    Schema[] schemaArray = new Schema[numberOfTypes];
    Object[] objectArray = new Object[numberOfTypes];
    int i = 0;
    for (DataType dataType : DataType.values()) {
      schemaArray[i] = new SchemaBuilder().setSchemaName("automatedTestSchema").addSingleValueDimension(columnName, dataType).build();
      i++;
    }

    objectArray[0] = new Boolean(true);
    objectArray[1] = new Byte((byte) 65);
    objectArray[2] = new Character('a');
    objectArray[3] = new Short((short) 500);
    objectArray[4] = new Integer(500);
    objectArray[5] = new Long(500);
    objectArray[6] = new Float(500.50);
    objectArray[7] = new Double(500.50);
    objectArray[8] = "Pinot Rules";
    objectArray[9] = null;
    objectArray[10] = new Byte[1];
    ((Byte[]) objectArray[10])[0] = new Byte((byte) 65);
    objectArray[11] = new Character[1];
    ((Character[]) objectArray[11])[0] = new Character('a');
    objectArray[12] = new Short[1];
    ((Short[]) objectArray[12])[0] = new Short((short) 500);
    objectArray[13] = new Integer[1];
    ((Integer[]) objectArray[13])[0] = new Integer(500);
    objectArray[14] = new Long[1];
    ((Long[]) objectArray[14])[0] = new Long(500);
    objectArray[15] = new Float[1];
    ((Float[]) objectArray[15])[0] = new Float(500.50);
    objectArray[16] = new Double[1];
    ((Double[]) objectArray[16])[0] = new Double(500.50);
    objectArray[17] = new String[1];
    ((String[]) objectArray[17])[0] = new String("Pinot Rules");

    for (i = 0; i < numberOfTypes; i++) {
      for (int j = 0; j < numberOfTypes; j++) {
        PlainFieldExtractor extractor =
            (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schemaArray[i]);
        GenericRow row = new GenericRow();
        Map<String, Object> fieldMap = new HashMap<String, Object>();
        fieldMap.put(columnName, objectArray[j]);
        row.init(fieldMap);
        extractor.transform(row);
        if (j == 9) {
          // Checking operations on null
          Assert.assertEquals(extractor.getTotalNulls(), 1);
        } else if ((i == 0) && (j != 0)) {
          // Checking non-Boolean to Boolean conversions
          if (j == 8) {
            // String to Boolean conversion
            Assert.assertEquals(extractor.getTotalErrors(), 0);
            Assert.assertEquals(extractor.getTotalConversions(), 1);
          } else {
            Assert.assertEquals(extractor.getTotalErrors(), 1);
            Assert.assertEquals(extractor.getTotalConversions(), 1);
          }
        }
        if ((i == 8) && (j == 0)) {
          // Boolean to String conversion
          Assert.assertEquals(extractor.getTotalErrors(), 0);
          Assert.assertEquals(extractor.getTotalConversions(), 1);
        }
        LOGGER.debug("Number of Error {}", extractor.getTotalErrors());
        LOGGER.debug("Number of rows with Null columns {}", extractor.getTotalNulls());
        LOGGER.debug("Number of rows with columns requiring conversion {}", extractor.getTotalConversions());
        LOGGER.debug("Column with conversion {}, number of conversions {}", columnName,
            extractor.getError_count().get(columnName));
        LOGGER.debug("Old value {}, new value {}", objectArray[j], row.getValue(columnName));
      }
    }
  }

  @Test
  public void timeSpecTest()
      throws Exception {
    Schema schema = new SchemaBuilder().addSingleValueDimension("svDimensionInt", DataType.INT)
        .addSingleValueDimension("svDimensionDouble", DataType.DOUBLE)
        .addMultiValueDimension("mvDimension", DataType.STRING, ",")
        .addMetric("metric", DataType.INT)
        .build();
    TimeFieldSpec timeSpec = new TimeFieldSpec();
    TimeGranularitySpec timeGranularitySpec = new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "incoming");
    timeSpec.setIncomingGranularitySpec(timeGranularitySpec);
    timeSpec.setOutgoingGranularitySpec(timeGranularitySpec);
    schema.setTimeFieldSpec(timeSpec);
    PlainFieldExtractor extractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    Short shortObj = new Short((short) 5);
    fieldMap.put("svDimensionInt", shortObj);
    Float floatObj = new Float((float) 3.2);
    fieldMap.put("svDimensionDouble", floatObj);
    Double doubleObj = new Double((double) 34.5);
    fieldMap.put("metric", doubleObj);
    row.init(fieldMap);
    extractor.transform(row);
    Assert.assertTrue(row.getValue("svDimensionInt") instanceof Integer);
    Assert.assertTrue(row.getValue("svDimensionDouble") instanceof Double);
    Assert.assertTrue(row.getValue("mvDimension") != null);
    Assert.assertTrue(row.getValue("metric") instanceof Integer);
    Assert.assertTrue((Integer) row.getValue("metric") == 34);
    Assert.assertTrue(row.getValue("incoming") != null);
  }

  @Test
  public void inNoutTimeSpecTest()
      throws Exception {
    Schema schema = new SchemaBuilder().addSingleValueDimension("svDimensionInt", DataType.INT)
        .addSingleValueDimension("svDimensionDouble", DataType.DOUBLE)
        .addMultiValueDimension("mvDimension", DataType.STRING, ",")
        .addMetric("metric", DataType.INT)
        .build();
    TimeFieldSpec timeSpec = new TimeFieldSpec();
    TimeGranularitySpec incomingTimeGranularitySpec = new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, "incoming");
    TimeGranularitySpec outgoingTimeGranularitySpec = new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "outgoing");
    timeSpec.setIncomingGranularitySpec(incomingTimeGranularitySpec);
    timeSpec.setOutgoingGranularitySpec(outgoingTimeGranularitySpec);
    schema.setTimeFieldSpec(timeSpec);
    PlainFieldExtractor extractor = (PlainFieldExtractor) FieldExtractorFactory.getPlainFieldExtractor(schema);
    GenericRow row = new GenericRow();
    Map<String, Object> fieldMap = new HashMap<String, Object>();
    Short shortObj = new Short((short) 5);
    fieldMap.put("svDimensionInt", shortObj);
    Float floatObj = new Float((float) 3.2);
    fieldMap.put("svDimensionDouble", floatObj);
    Double doubleObj = new Double((double) 34.5);
    fieldMap.put("metric", doubleObj);
    long currentDaysSinceEpoch = System.currentTimeMillis() / 1000 / 60 / 60 / 24;
    fieldMap.put("incoming", currentDaysSinceEpoch);
    row.init(fieldMap);
    extractor.transform(row);
    Assert.assertTrue(row.getValue("svDimensionInt") instanceof Integer);
    Assert.assertTrue(row.getValue("svDimensionDouble") instanceof Double);
    Assert.assertTrue(row.getValue("mvDimension") != null);
    Assert.assertTrue(row.getValue("metric") instanceof Integer);
    Assert.assertTrue((Integer) row.getValue("metric") == 34);
    Assert.assertTrue(row.getValue("incoming") == null);
    Assert.assertEquals(((Long) row.getValue("outgoing")).longValue(), currentDaysSinceEpoch * 24);
  }
}
