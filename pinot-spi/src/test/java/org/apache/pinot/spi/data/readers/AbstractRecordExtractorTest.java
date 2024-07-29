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

import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests the RecordReader for schema with transform functions
 */
public abstract class AbstractRecordExtractorTest {

  protected Set<String> _sourceFieldNames;
  protected List<Map<String, Object>> _inputRecords;
  private RecordReader _recordReader;
  private RecordReader _recordReaderNoIncludeList;
  protected final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getName());

  @BeforeClass
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
    _sourceFieldNames = getSourceFields();
    _inputRecords = getInputRecords();
    createInputFile();
    _recordReader = createRecordReader(_sourceFieldNames);
    _recordReaderNoIncludeList = createRecordReader(null);
  }

  private static Map<String, String> createMap(Pair<String, String>[] entries) {
    Map<String, String> map = new HashMap<>();
    for (Pair<String, String> entry : entries) {
      map.put(entry.getLeft(), entry.getRight());
    }
    return map;
  }

  protected List<Map<String, Object>> getInputRecords() {
    Integer[] userID = new Integer[]{1, 2, null, 4};
    String[] firstName = new String[]{null, "John", "Ringo", "George"};
    String[] lastName = new String[]{"McCartney", "Lenon", "Starr", "Harrison"};
    List[] bids = new List[]{Arrays.asList(10, 20), null, Collections.singletonList(1), Arrays.asList(1, 2, 3)};
    String[] campaignInfo = new String[]{"yesterday", "blackbird", "here comes the sun", "hey jude"};
    double[] cost = new double[]{10000, 20000, 30000, 25000};
    long[] timestamp = new long[]{1570863600000L, 1571036400000L, 1571900400000L, 1574000000000L};
    List[] arrays = new List[]{Arrays.asList("a", "b", "c"), Arrays.asList("d", "e"), Arrays.asList("w", "x", "y",
        "z"), Collections.singletonList("a")};
    Map<String, String>[] maps = new Map[]{
        createMap(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
        createMap(new Pair[]{Pair.of("a", "3"), Pair.of("b", "4")}),
        createMap(new Pair[]{Pair.of("a", "5"), Pair.of("b", "6")}),
        createMap(new Pair[]{Pair.of("a", "7"), Pair.of("b", "8")})
    };

    List<Map<String, Object>> inputRecords = new ArrayList<>(4);
    for (int i = 0; i < 4; i++) {
      Map<String, Object> record = new HashMap<>();
      record.put("user_id", userID[i]);
      record.put("firstName", firstName[i]);
      record.put("lastName", lastName[i]);
      record.put("bids", bids[i]);
      record.put("campaignInfo", campaignInfo[i]);
      record.put("cost", cost[i]);
      record.put("timestamp", timestamp[i]);
      record.put("xarray", arrays[i]);
      record.put("xmap", maps[i]);
      inputRecords.add(record);
    }
    return inputRecords;
  }

  protected Set<String> getSourceFields() {
    return Sets.newHashSet("user_id", "firstName", "lastName", "bids", "campaignInfo", "cost", "timestamp", "xarray",
        "xmap");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.forceDelete(_tempDir);
  }

  protected abstract RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException;

  protected abstract void createInputFile()
      throws IOException;

  protected void checkValue(Map<String, Object> inputRecord, GenericRow genericRow) {
    for (Map.Entry<String, Object> entry : inputRecord.entrySet()) {
      String columnName = entry.getKey();
      Object expectedValue = entry.getValue();
      Object actualValue = genericRow.getValue(columnName);
      checkValue(expectedValue, actualValue);
    }
  }

  private void checkValue(Object expectedValue, Object actualValue) {
    if (expectedValue instanceof Collection) {
      List actualArray = actualValue instanceof List ? (ArrayList) actualValue : Arrays.asList((Object[]) actualValue);
      List expectedArray = (List) expectedValue;
      for (int j = 0; j < actualArray.size(); j++) {
        checkValue(expectedArray.get(j), actualArray.get(j));
      }
    } else if (expectedValue instanceof Map) {
      Map<Object, Object> actualMap = (HashMap) actualValue;
      Map<Object, Object> expectedMap = (HashMap) expectedValue;
      for (Map.Entry<Object, Object> mapEntry : expectedMap.entrySet()) {
        checkValue(mapEntry.getValue(), actualMap.get(mapEntry.getKey().toString()));
      }
    } else if (expectedValue instanceof GenericData.Record) {
      Map<Object, Object> actualMap = (HashMap) actualValue;
      GenericData.Record expectedGenericRecord = (GenericData.Record) expectedValue;
      for (Map.Entry<Object, Object> mapEntry : actualMap.entrySet()) {
        checkValue(expectedGenericRecord.get(mapEntry.getKey().toString()), mapEntry.getValue());
      }
    } else {
      if (expectedValue != null) {
        Assert.assertEquals(actualValue, expectedValue);
      } else {
        Assert.assertNull(actualValue);
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
    if (_recordReaderNoIncludeList != null) {
      _recordReaderNoIncludeList.rewind();
      i = 0;
      while (_recordReaderNoIncludeList.hasNext()) {
        _recordReaderNoIncludeList.next(genericRow);
        Map<String, Object> inputRecord = _inputRecords.get(i++);
        checkValue(inputRecord, genericRow);
      }
    }
  }
}
