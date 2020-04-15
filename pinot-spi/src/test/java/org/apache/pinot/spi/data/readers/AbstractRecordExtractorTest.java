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
package org.apache.pinot.spi.data.readers;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.SchemaFieldExtractorUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the RecordReader for schema with groovy transform functions
 */
public abstract class AbstractRecordExtractorTest {

  protected Schema _pinotSchema;
  protected List<String> _sourceFieldNames;
  protected List<Map<String, Object>> _inputRecords;
  private RecordReader _recordReader;
  protected final File _tempDir = new File(FileUtils.getTempDirectory(), "RecordTransformationTest");

  @BeforeClass
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
    _pinotSchema = getPinotSchema();
    _sourceFieldNames = SchemaFieldExtractorUtils.extractSource(_pinotSchema);
    _inputRecords = getInputRecords();
    createInputFile();
    _recordReader = createRecordReader();
  }

  protected Schema getPinotSchema()
      throws IOException {
    URL resource =
        AbstractRecordExtractorTest.class.getClassLoader().getResource("groovy_transform_functions_schema.json");
    File schemaFile = new File(resource.getFile());
    return Schema.fromFile(schemaFile);
  }

  protected List<Map<String, Object>> getInputRecords() {
    Integer[] userID = new Integer[]{1, 2, null, 4};
    String[] firstName = new String[]{null, "John", "Ringo", "George"};
    String[] lastName = new String[]{"McCartney", "Lenon", "Starr", "Harrison"};
    List<List<Integer>> bids = new ArrayList<>(4);
    bids.add(new ArrayList<>(Arrays.asList(10, 20)));
    bids.add(null);
    bids.add(new ArrayList<>(Arrays.asList(1)));
    bids.add(new ArrayList<>(Arrays.asList(1, 2, 3)));
    byte[][] campaignInfo =
        new byte[][]{"yesterday".getBytes(), "blackbird".getBytes(), "here comes the sun".getBytes(), "hey jude".getBytes()};
    double[] cost = new double[]{10000, 20000, 30000, 25000};
    long[] timestamp = new long[]{1570863600000L, 1571036400000L, 1571900400000L, 1574000000000L};

    List<Map<String, Object>> inputRecords = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      Map<String, Object> record = new HashMap<>();
      record.put("user_id", userID[i]);
      record.put("firstName", firstName[i]);
      record.put("lastName", lastName[i]);
      record.put("bids", bids.get(i));
      record.put("campaignInfo", campaignInfo[i].toString());
      record.put("cost", cost[i]);
      record.put("timestamp", timestamp[i]);
      inputRecords.add(record);
    }
    return inputRecords;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(_tempDir);
  }

  protected abstract RecordReader createRecordReader()
      throws IOException;

  protected abstract void createInputFile()
      throws IOException;

  protected void checkValue(Map<String, Object> inputRecord, GenericRow genericRow) {
    for (Map.Entry<String, Object> entry : inputRecord.entrySet()) {
      String columnName = entry.getKey();
      Object expectedValue = entry.getValue();
      Object actualValue = genericRow.getValue(columnName);
      if (expectedValue instanceof Collection) {
        List actualArray =
            actualValue instanceof List ? (ArrayList) actualValue : Arrays.asList((Object[]) actualValue);
        List expectedArray = (ArrayList) expectedValue;
        for (int j = 0; j < actualArray.size(); j++) {
          Assert.assertEquals(actualArray.get(j), expectedArray.get(j));
        }
      } else if (expectedValue instanceof Map) {
        Map<Object, Object> actualMap = (HashMap) actualValue;
        Map<Object, Object> expectedMap = (HashMap) expectedValue;
        for (Map.Entry<Object, Object> mapEntry : expectedMap.entrySet()) {
          Assert.assertEquals(actualMap.get(mapEntry.getKey().toString()), mapEntry.getValue());
        }
      } else {
        if (expectedValue != null) {
          Assert.assertEquals(actualValue, expectedValue);
        } else {
          Assert.assertNull(actualValue);
        }
      }
    }
  }

  /**
   * Tests the record reader using a schema with Groovy transform functions.
   * The record reader should output records which have all the source fields.
   */
  @Test
  public void testRecordExtractor()
      throws IOException {
    _recordReader.rewind();
    GenericRow genericRow = new GenericRow();
    int i = 0;
    while (_recordReader.hasNext()) {
      _recordReader.next(genericRow);
      Map<String, Object> inputRecord = _inputRecords.get(i++);
      checkValue(inputRecord, genericRow);
    }
  }
}
