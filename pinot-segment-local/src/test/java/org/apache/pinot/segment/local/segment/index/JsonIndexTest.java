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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexType;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit test for {@link JsonIndexCreator} and {@link JsonIndexReader}.
 */
public class JsonIndexTest implements PinotBuffersAfterMethodCheckRule {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIndexTest");
  private static final String ON_HEAP_COLUMN_NAME = "onHeap";
  private static final String OFF_HEAP_COLUMN_NAME = "offHeap";
  public static final String TEST_RECORD = "{"
      + "\"name\": \"adam\","
      + "\"age\": 20,"
      + "\"addresses\": ["
      + "  {"
      + "    \"country\": \"us\","
      + "    \"street\": \"main st\","
      + "    \"number\": 1"
      + "  },"
      + "  {"
      + "    \"country\": \"ca\","
      + "    \"street\": \"second st\","
      + "    \"number\": 2"
      + "  }"
      + "],"
      + "\"skills\": ["
      + "  \"english\","
      + "  \"programming\""
      + "]"
      + "}";

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testSmallIndex()
      throws Exception {
    // @formatter: off
    // CHECKSTYLE:OFF
    String[] records = new String[]{
        "{" + "\"name\":\"adam\"," + "\"age\":20," + "\"score\":1.25," + "\"addresses\":["
            + "   {\"street\":\"street-00\",\"country\":\"us\"}," + "   {\"street\":\"street-01\",\"country\":\"us\"},"
            + "   {\"street\":\"street-02\",\"country\":\"ca\"}]," + "\"skills\":[\"english\",\"programming\"]" + "}",
        "{" + "\"name\":\"bob\"," + "\"age\":25," + "\"score\":1.94," + "\"addresses\":["
            + "   {\"street\":\"street-10\",\"country\":\"ca\"}," + "   {\"street\":\"street-11\",\"country\":\"us\"},"
            + "   {\"street\":\"street-12\",\"country\":\"in\"}]," + "\"skills\":[]" + "}",
        "{" + "\"name\":\"charles\"," + "\"age\":30," + "\"score\":0.90,"  + "\"addresses\":["
            + "   {\"street\":\"street-20\",\"country\":\"jp\"}," + "   {\"street\":\"street-21\",\"country\":\"kr\"},"
            + "   {\"street\":\"street-22\",\"country\":\"cn\"}]," + "\"skills\":[\"japanese\",\"korean\",\"chinese\"]"
            + "}", "{" + "\"name\":\"david\"," + "\"age\":35," + "\"score\":0.9999,"  + "\"addresses\":["
        + "   {\"street\":\"street-30\",\"country\":\"ca\",\"types\":[\"home\",\"office\"]},"
        + "   {\"street\":\"street-31\",\"country\":\"ca\"}," + "   {\"street\":\"street-32\",\"country\":\"ca\"}],"
        + "\"skills\":null" + "}"
    };
    //CHECKSTYLE:ON
    // @formatter: on
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }
      JsonIndexReader[] indexReaders = new JsonIndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableJsonIndex};
      for (JsonIndexReader indexReader : indexReaders) {
        MutableRoaringBitmap matchingDocIds = getMatchingDocIds(indexReader, "name='bob'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" = 'street-21'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader, "REGEXP_LIKE(\"addresses[*].street\", 'street-2.*')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"age\" > 25");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"age\" >= 25");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"age\" < 25");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader, "\"age\" <= 25");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"name\" > 'adam'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"name\" > 'a'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"score\" > 1");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"score\" > 1.0");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"score\" > 0.99");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "REGEXP_LIKE(\"score\", '[0-1]\\.[6-9].*')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" NOT IN ('street-10', 'street-22')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].country\" != 'ca'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"skills[*]\" NOT IN ('english', 'japanese')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].country\" IN ('ca', 'us')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].country\" NOT IN ('ca', 'us')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[1]\" = 'office'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].types[0]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[*]\" IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader, "abc IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 2});

        matchingDocIds =
            getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'ca' AND \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'us' OR \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});

        // Nested exclusive predicates

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"addresses[0].street\" = 'street-00' AND \"addresses[0].country\" != 'ca'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"age\" = '20' AND \"addresses[*].country\" NOT IN ('us')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"age\" = '20' AND \"addresses[*].country\" NOT IN ('us', 'ca')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"addresses[*].street\" = 'street-21' AND \"addresses[*].country\" != 'kr'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"addresses[*].street\" = 'street-21' AND \"addresses[*].country\" != 'us'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader,
            "\"addresses[*].street\" = 'street-30' AND \"addresses[*].country\" NOT IN ('us', 'kr')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader,
            "REGEXP_LIKE(\"addresses[*].street\", 'street-0.*') AND \"addresses[*].country\" NOT IN ('us', 'ca')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader,
            "REGEXP_LIKE(\"addresses[*].street\", 'street-3.*') AND \"addresses[*].country\" != 'us'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        // A single matching flattened doc ID will result in the overall doc being matched
        matchingDocIds = getMatchingDocIds(indexReader,
            "\"addresses[*].street\" = 'street-21' AND \"skills[*]\" != 'japanese'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});
      }
    }
  }

  @Test
  public void testLargeIndex()
      throws Exception {
    int numRecords = 123_456;
    String[] records = new String[numRecords];
    for (int i = 0; i < numRecords; i++) {
      records[i] = String.format(
          "{\"name\":\"adam-%d\",\"addresses\":[{\"street\":\"us-%d\",\"country\":\"us\"},{\"street\":\"ca-%d\","
              + "\"country\":\"ca\"}]}", i, i, i);
    }
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }
      JsonIndexReader[] indexReaders = new JsonIndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableJsonIndex};
      for (JsonIndexReader indexReader : indexReaders) {
        MutableRoaringBitmap matchingDocIds = getMatchingDocIds(indexReader, "name = 'adam-123'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{123});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" = 'us-456'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{456});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].street\" = 'us-456'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds =
            getMatchingDocIds(indexReader, "\"addresses[*].street\" = 'us-456' AND \"addresses[*].country\" = 'ca'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader,
            "name = 'adam-100000' AND \"addresses[*].street\" = 'us-100000' AND \"addresses[*].country\" = 'us'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{100000});

        matchingDocIds =
            getMatchingDocIds(indexReader, "name = 'adam-100000' AND \"addresses[*].street\" = 'us-100001'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader, "name != 'adam-100000'");
        Assert.assertEquals(matchingDocIds.getCardinality(), 123_455);
      }
    }
  }

  @Test
  public void testFilteringLongValues()
      throws Exception {
    String[] records = new String[]{
        "{\"key1\":\"value1\",\"key2\":\"longValue2\",\"nestedKey3\":{\"key4\":\"longValue4\"}}",
        "{\"key5\":\"longValue5\",\"key6\":\"value6\",\"nestedKey7\":{\"key8\":\"value8\"}}"
    };
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setMaxValueLength(6);

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }

      JsonIndexReader[] indexReaders = new JsonIndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableJsonIndex};
      for (JsonIndexReader indexReader : indexReaders) {
        MutableRoaringBitmap matchingDocIds = getMatchingDocIds(indexReader, "key1='value1'");
        Assert.assertEquals(new int[]{0}, matchingDocIds.toArray());

        matchingDocIds = getMatchingDocIds(indexReader, "key2='longValue2'");
        Assert.assertTrue(matchingDocIds.isEmpty());
        matchingDocIds = getMatchingDocIds(indexReader, "key2='" + JsonUtils.SKIPPED_VALUE_REPLACEMENT + "'");
        Assert.assertEquals(new int[]{0}, matchingDocIds.toArray());

        matchingDocIds = getMatchingDocIds(indexReader, "nestedKey3.key4='longValue4'");
        Assert.assertTrue(matchingDocIds.isEmpty());
        matchingDocIds =
            getMatchingDocIds(indexReader, "nestedKey3.key4='" + JsonUtils.SKIPPED_VALUE_REPLACEMENT + "'");
        Assert.assertEquals(new int[]{0}, matchingDocIds.toArray());

        matchingDocIds = getMatchingDocIds(indexReader, "key5='longValue5'");
        Assert.assertTrue(matchingDocIds.isEmpty());
        matchingDocIds = getMatchingDocIds(indexReader, "key5='" + JsonUtils.SKIPPED_VALUE_REPLACEMENT + "'");
        Assert.assertEquals(new int[]{1}, matchingDocIds.toArray());

        matchingDocIds = getMatchingDocIds(indexReader, "key6='value6'");
        Assert.assertEquals(new int[]{1}, matchingDocIds.toArray());

        matchingDocIds = getMatchingDocIds(indexReader, "nestedKey7.key8='value8'");
        Assert.assertEquals(new int[]{1}, matchingDocIds.toArray());
      }
    }
  }

  /**
   * Creates a JSON index with the given config and adds the given records
   * @param createOnHeap Whether to create an on-heap index
   * @param jsonIndexConfig the JSON index config
   * @param records the records to be added to the index
   * @throws IOException on error
   */
  private void createIndex(boolean createOnHeap, JsonIndexConfig jsonIndexConfig, String[] records)
      throws IOException {
    try (JsonIndexCreator indexCreator = createOnHeap
        ? new OnHeapJsonIndexCreator(INDEX_DIR, ON_HEAP_COLUMN_NAME, jsonIndexConfig)
        : new OffHeapJsonIndexCreator(INDEX_DIR, OFF_HEAP_COLUMN_NAME, jsonIndexConfig)) {
      for (String record : records) {
        indexCreator.add(record);
      }
      indexCreator.seal();
    }
  }

  private MutableRoaringBitmap getMatchingDocIds(JsonIndexReader indexReader, String filter) {
    return indexReader.getMatchingDocIds(filter);
  }

  @Test
  public void testGetValueToFlattenedDocIdsMap()
      throws Exception {
    // @formatter: off
    // CHECKSTYLE:OFF
    String[] records = new String[]{
        "{\"arrField\": " + "[{\"intKey01\": 1, \"stringKey01\": \"abc\"},"
            + " {\"intKey01\": 1, \"stringKey01\": \"foo\"}, " + " {\"intKey01\": 3, \"stringKey01\": \"bar\"},"
            + " {\"intKey01\": 5, \"stringKey01\": \"fuzz\"}]}",
        "{\"arrField\": " + "[{\"intKey01\": 7, \"stringKey01\": \"pqrS\"},"
            + " {\"intKey01\": 6, \"stringKey01\": \"foo\"}, " + " {\"intKey01\": 8, \"stringKey01\": \"test\"},"
            + " {\"intKey01\": 9, \"stringKey01\": \"testf2\"}]}",
        "{\"arrField\": " + "[{\"intKey01\": 1, \"stringKey01\": \"pqr\"},"
            + " {\"intKey01\": 1, \"stringKey01\": \"foo\"}, " + " {\"intKey01\": 6, \"stringKey01\": \"test\"},"
            + " {\"intKey01\": 3, \"stringKey01\": \"testf2\"}]}",
    };
    // CHECKSTYLE:ON
    // @formatter: on

    String[][] testKeys = new String[][]{
        // Without filters
        {"$.arrField[*].intKey01", null},
        {"$.arrField[*].stringKey01", null},

        // With regexp filter
        {"$.arrField[*].intKey01", "REGEXP_LIKE(\"arrField..stringKey01\", '.*f.*')"},
        // With range filter
        {"$.arrField[*].stringKey01", "\"arrField..intKey01\" > 2"},
        // With AND filters
        {"$.arrField[*].intKey01", "\"arrField..intKey01\" > 2 AND REGEXP_LIKE(\"arrField..stringKey01\", "
            + "'[a-b][a-b].*')"},
        // Exclusive filters
        {"$.arrField[*].intKey01", "\"arrField[*].stringKey01\" != 'bar'"},
        {"$.arrField[*].stringKey01", "\"arrField[*].intKey01\" != '3'"},
    };

    String colName = "col";
    try (JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(INDEX_DIR, colName, new JsonIndexConfig());
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(new JsonIndexConfig())) {
      for (String record : records) {
        offHeapIndexCreator.add(record);
        mutableJsonIndex.add(record);
      }
      offHeapIndexCreator.seal();

      File offHeapIndexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
      Assert.assertTrue(offHeapIndexFile.exists());

      try (PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
          ImmutableJsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer,
              records.length)) {
        int[] docMask = new int[]{0, 2, 1};
        int docIdValidLength = 2;
        String[][][] expectedValues = new String[][][]{
            {{"1", "1", "3", "5"}, {"1", "1", "6", "3"}},
            {{"abc", "foo", "bar", "fuzz"}, {"pqr", "foo", "test", "testf2"}},
            {{"1", "5"}, {"1", "3"}},
            {{"bar", "fuzz"}, {"test", "testf2"}},
            {{"3"}, {}},
            {{"1", "1", "5"}, {"1", "1", "6", "3"}},
            {{"abc", "foo", "fuzz"}, {"pqr", "foo", "test"}}
        };
        for (int i = 0; i < testKeys.length; i++) {
          Map<String, RoaringBitmap> context =
              offHeapIndexReader.getMatchingFlattenedDocsMap(testKeys[i][0], testKeys[i][1]);
          String[][] values = offHeapIndexReader.getValuesMV(docMask, docIdValidLength, context);

          for (int j = 0; j < docIdValidLength; j++) {
            Assert.assertEquals(values[j], expectedValues[i][j]);
          }

          context = mutableJsonIndex.getMatchingFlattenedDocsMap(testKeys[i][0], testKeys[i][1]);
          values = mutableJsonIndex.getValuesMV(docMask, docIdValidLength, context);
          Assert.assertEquals(values, expectedValues[i]);
        }
      }
    }
  }

  @Test
  public void testGetValuesForKeyAndDocs()
      throws Exception {
    // @formatter: off
    // CHECKSTYLE:OFF
    String[] records = new String[]{
        "{\"field1\":\"value1\",\"field2\":\"value2\",\"field3\":\"value3\"}",
        "{\"field1\":\"value2\", \"field2\":[\"value1\",\"value2\"]}",
        "{\"field1\":\"value1\",\"field2\":\"value4\"}",
    };
    // CHECKSTYLE:ON
    // @formatter: on
    String[] testKeys = new String[]{"$.field1", "$.field2", "$.field3", "$.field4"};

    String colName = "col";
    try (
        JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(INDEX_DIR, colName, new JsonIndexConfig());
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(new JsonIndexConfig())) {
      for (String record : records) {
        offHeapIndexCreator.add(record);
        mutableJsonIndex.add(record);
      }
      offHeapIndexCreator.seal();

      File offHeapIndexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
      Assert.assertTrue(offHeapIndexFile.exists());

      try (PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
          ImmutableJsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer,
              records.length)) {

        // No filtering
        int[] docMask = new int[]{0, 1, 2};
        String[][] expectedValues =
            new String[][]{{"value1", "value2", "value1"}, {"value2", null, "value4"}, {"value3", null, null},
                {null, null, null}};
        for (int i = 0; i < testKeys.length; i++) {
          Map<String, RoaringBitmap> context = offHeapIndexReader.getMatchingFlattenedDocsMap(testKeys[i], null);
          String[] values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, true);
          Assert.assertEquals(values, expectedValues[i]);

          offHeapIndexReader.convertFlattenedDocIdsToDocIds(context);
          values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, false);
          Assert.assertEquals(values, expectedValues[i]);

          context = mutableJsonIndex.getMatchingFlattenedDocsMap(testKeys[i], null);
          values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, true);
          Assert.assertEquals(values, expectedValues[i]);

          mutableJsonIndex.convertFlattenedDocIdsToDocIds(context);
          values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, false);
          Assert.assertEquals(values, expectedValues[i]);
        }

        // Some filtering
        docMask = new int[]{1, 2};
        expectedValues = new String[][]{{"value2", "value1"}, {null, "value4"}, {null, null}, {null, null}};
        for (int i = 0; i < testKeys.length; i++) {
          Map<String, RoaringBitmap> context = offHeapIndexReader.getMatchingFlattenedDocsMap(testKeys[i], null);
          String[] values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, true);
          Assert.assertEquals(values, expectedValues[i]);

          offHeapIndexReader.convertFlattenedDocIdsToDocIds(context);
          values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, false);
          Assert.assertEquals(values, expectedValues[i]);

          context = mutableJsonIndex.getMatchingFlattenedDocsMap(testKeys[i], null);
          values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, true);
          Assert.assertEquals(values, expectedValues[i]);

          mutableJsonIndex.convertFlattenedDocIdsToDocIds(context);
          values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, false);
          Assert.assertEquals(values, expectedValues[i]);
        }

        // Immutable index, context is reused for the second method call
        Map<String, RoaringBitmap> context = offHeapIndexReader.getMatchingFlattenedDocsMap("$.field1", null);
        docMask = new int[]{0};
        String[] values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, true);
        Assert.assertEquals(values, new String[]{"value1"});
        docMask = new int[]{1, 2};
        values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, true);
        Assert.assertEquals(values, new String[]{"value2", "value1"});

        offHeapIndexReader.convertFlattenedDocIdsToDocIds(context);
        docMask = new int[]{0};
        values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, false);
        Assert.assertEquals(values, new String[]{"value1"});
        docMask = new int[]{1, 2};
        values = offHeapIndexReader.getValuesSV(docMask, docMask.length, context, false);
        Assert.assertEquals(values, new String[]{"value2", "value1"});

        // Mutable index, context is reused for the second method call
        context = mutableJsonIndex.getMatchingFlattenedDocsMap("$.field1", null);
        docMask = new int[]{0};
        values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, true);
        Assert.assertEquals(values, new String[]{"value1"});
        docMask = new int[]{1, 2};
        values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, true);
        Assert.assertEquals(values, new String[]{"value2", "value1"});

        mutableJsonIndex.convertFlattenedDocIdsToDocIds(context);
        docMask = new int[]{0};
        values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, false);
        Assert.assertEquals(values, new String[]{"value1"});
        docMask = new int[]{1, 2};
        values = mutableJsonIndex.getValuesSV(docMask, docMask.length, context, false);
        Assert.assertEquals(values, new String[]{"value2", "value1"});
      }
    }
  }

  @Test
  public void testWhenDisableCrossArrayUnnestIsOffThenJsonArraysAreSeparated()
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setDisableCrossArrayUnnest(true);

    List<Map<String, String>> result = JsonUtils.flatten(TEST_RECORD, jsonIndexConfig);

    Assert.assertEquals(result.toString(),
        "["
            + "{.addresses.$index=0, .addresses..country=us, .addresses..number=1, .addresses..street=main st, "
            + ".age=20, .name=adam}, "
            + "{.addresses.$index=1, .addresses..country=ca, .addresses..number=2, .addresses..street=second st, "
            + ".age=20, .name=adam}, "
            + "{.age=20, .name=adam, .skills.=english, .skills.$index=0}, "
            + "{.age=20, .name=adam, .skills.=programming, .skills.$index=1}]");
  }

  @Test
  public void testWhenDisableCrossArrayUnnestIsOnThenJsonArraysAreCombined()
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setDisableCrossArrayUnnest(false);

    List<Map<String, String>> result = JsonUtils.flatten(TEST_RECORD, jsonIndexConfig);

    Assert.assertEquals(result.toString(),
        "["
            + "{.addresses.$index=0, .addresses..country=us, .addresses..number=1, .addresses..street=main st, "
            + ".age=20, .name=adam, "
            + ".skills.=english, .skills.$index=0}, "
            + "{.addresses.$index=0, .addresses..country=us, .addresses..number=1, .addresses..street=main st, "
            + ".age=20, .name=adam, "
            + ".skills.=programming, .skills.$index=1}, "
            + "{.addresses.$index=1, .addresses..country=ca, .addresses..number=2, .addresses..street=second st, "
            + ".age=20, .name=adam, "
            + ".skills.=english, .skills.$index=0}, "
            + "{.addresses.$index=1, .addresses..country=ca, .addresses..number=2, .addresses..street=second st, "
            + ".age=20, .name=adam, "
            + ".skills.=programming, .skills.$index=1}]");
  }

  @Test
  public void testWhenDisableCrossArrayUnnestIsOnThenQueriesOnMultipleArraysReturnEmptyResult()
      throws IOException {
    RoaringBitmap expectedBitmap = RoaringBitmap.bitmapOf();
    boolean disableCrossArrayUnnest = true;

    assertWhenCrossArrayUnnestIs(disableCrossArrayUnnest, expectedBitmap);
  }

  @Test
  public void testWhenDisableCrossArrayUnnestIsOffThenQueriesOnMultipleArraysReturnGoodResult()
      throws IOException {
    RoaringBitmap expectedBitmap = RoaringBitmap.bitmapOf(0);
    boolean disableCrossArrayUnnest = false;

    assertWhenCrossArrayUnnestIs(disableCrossArrayUnnest, expectedBitmap);
  }

  private void assertWhenCrossArrayUnnestIs(boolean disableCrossArrayUnnest, RoaringBitmap expectedBitmap)
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setDisableCrossArrayUnnest(disableCrossArrayUnnest);

    String[] records = {TEST_RECORD};

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndex = new ImmutableJsonIndexReader(onHeapBuffer, records.length);
        JsonIndexReader offHeapIndex = new ImmutableJsonIndexReader(offHeapBuffer, records.length);
        MutableJsonIndexImpl mutableIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableIndex.add(record);
      }

      String filter = "\"$.addresses[*].country\" = 'us' and \"$.skills[*]\" = 'english'";

      assertEquals(onHeapIndex.getMatchingDocIds(filter), expectedBitmap);
      assertEquals(offHeapIndex.getMatchingDocIds(filter), expectedBitmap);
      assertEquals(mutableIndex.getMatchingDocIds(filter), expectedBitmap);
    }
  }

  @Test
  public void testWhenDisableCrossArrayUnnestIsOnThenJsonFlatteningBreaksWhen100kCombinationLimitIsExceeded()
      throws IOException {
    // flattening record with arrays whose combinations reach 100k returns exception
    StringBuilder record = generateRecordWith100kArrayElementCombinations();

    try {
      JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
      jsonIndexConfig.setDisableCrossArrayUnnest(false);
      createIndex(true, jsonIndexConfig, new String[]{record.toString()});
      Assert.fail("expected exception");
    } catch (IllegalArgumentException e) {
      assertEquals(e.getCause().getMessage(), "Got too many combinations");
    }
  }

  private static StringBuilder generateRecordWith100kArrayElementCombinations() {
    StringBuilder record = new StringBuilder();
    record.append('{');

    //address
    record.append("\n \"addresses\": [");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        record.append(',');
      }
      record.append("{ ")
          .append(" \"street\": \"").append("st").append(i).append("\"")
          .append(" }");
    }
    record.append("],");

    //skill
    record.append("\n \"skills\": [");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        record.append(',');
      }
      record.append("\"skill").append(i).append("\"");
    }
    record.append("],");

    //hobby
    record.append("\n \"hobbies\": [");
    for (int i = 0; i < 10; i++) {
      if (i > 0) {
        record.append(',');
      }
      record.append("\"hobby").append(i).append("\"");
    }
    record.append(']');
    record.append("\n}");
    return record;
  }

  @Test
  public void testSettingMaxValueLengthCausesLongValuesToBeReplacedWithSKIPPED()
      throws IOException {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setMaxValueLength(10);
    // value is longer than max length
    String[] records = {"{\"key1\":\"value_is_longer_than_10_characters\"}"};

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndex = new ImmutableJsonIndexReader(onHeapBuffer, records.length);
        JsonIndexReader offHeapIndex = new ImmutableJsonIndexReader(offHeapBuffer, records.length);
        MutableJsonIndexImpl mutableIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableIndex.add(record);
      }

      Map<String, RoaringBitmap> expectedMap = Collections.singletonMap(JsonUtils.SKIPPED_VALUE_REPLACEMENT,
          RoaringBitmap.bitmapOf(0));

      assertEquals(expectedMap, getMatchingDocsMap(onHeapIndex, "$.key1"));
      assertEquals(expectedMap, getMatchingDocsMap(offHeapIndex, "$.key1"));
      assertEquals(expectedMap, getMatchingDocsMap(mutableIndex, "$.key1"));

      // skipped values can be found for the key
      String filter = "\"$.key1\"='" + JsonUtils.SKIPPED_VALUE_REPLACEMENT + "'";

      RoaringBitmap expectedBitmap = RoaringBitmap.bitmapOf(0);
      assertEquals(expectedBitmap, onHeapIndex.getMatchingDocIds(filter));
      assertEquals(expectedBitmap, offHeapIndex.getMatchingDocIds(filter));
      assertEquals(expectedBitmap, mutableIndex.getMatchingDocIds(filter));
    }
  }

  private static Map<String, RoaringBitmap> getMatchingDocsMap(JsonIndexReader onHeapIndex, String key) {
    return onHeapIndex.getMatchingFlattenedDocsMap(key, null);
  }

  @Test
  public void testSkipInvalidJsonEnable() throws Exception {
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    jsonIndexConfig.setSkipInvalidJson(true);
    // the braces don't match and cannot be parsed
    String[] records = {"{\"key1\":\"va\""};

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }
      Map<String, RoaringBitmap> onHeapRes = getMatchingDocsMap(onHeapIndexReader, "$");
      Map<String, RoaringBitmap> offHeapRes = getMatchingDocsMap(offHeapIndexReader, "$");
      Map<String, RoaringBitmap> mutableRes = mutableJsonIndex.getMatchingFlattenedDocsMap("$", null);
      Map<String, RoaringBitmap> expectedRes = Collections.singletonMap(JsonUtils.SKIPPED_VALUE_REPLACEMENT,
          RoaringBitmap.bitmapOf(0));
      Assert.assertEquals(expectedRes, onHeapRes);
      Assert.assertEquals(expectedRes, offHeapRes);
      Assert.assertEquals(expectedRes, mutableRes);
    }
  }

  @Test(expectedExceptions = JsonProcessingException.class)
  public void testSkipInvalidJsonDisabled() throws Exception {
    // by default, skipInvalidJson is disabled
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();
    // the braces don't match and cannot be parsed
    String[] records = {"{\"key1\":\"va\""};

    createIndex(true, jsonIndexConfig, records);
  }


  @Test
  public void testGetMatchingValDocIdsPairForArrayPath() throws Exception {
    String[] records = {
            "{\"foo\":[{\"bar\":[\"x\",\"y\"]},{\"bar\":[\"a\",\"b\"]}],\"foo2\":[\"u\"]}",
            "{\"foo\":[{\"bar\":[\"y\",\"z\"]}],\"foo2\":[\"u\"]}"
    };
    JsonIndexConfig jsonIndexConfig = new JsonIndexConfig();

    createIndex(true, jsonIndexConfig, records);
    File onHeapIndexFile = new File(INDEX_DIR, ON_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    createIndex(false, jsonIndexConfig, records);
    File offHeapIndexFile = new File(INDEX_DIR, OFF_HEAP_COLUMN_NAME + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    String[] keys = {"$.foo[0].bar[1]", "$.foo[1].bar[0]", "$.foo2[0]", "$.foo[100].bar[100]", "$.foo[0].bar[*]",
            "$.foo[*].bar[0]", "$.foo[*].bar[*]"};
    List<Map<String, RoaringBitmap>> expected = List.of(
            Map.of("y", RoaringBitmap.bitmapOf(0), "z", RoaringBitmap.bitmapOf(1)),
            Map.of("a", RoaringBitmap.bitmapOf(0)),
            Map.of("u", RoaringBitmap.bitmapOf(0, 1)),
            Collections.emptyMap(),
            Map.of("x", RoaringBitmap.bitmapOf(0),
                    "y", RoaringBitmap.bitmapOf(0, 1),
                    "z", RoaringBitmap.bitmapOf(1)),
            Map.of("x", RoaringBitmap.bitmapOf(0),
                    "a", RoaringBitmap.bitmapOf(0),
                    "y", RoaringBitmap.bitmapOf(1)),
            Map.of("x", RoaringBitmap.bitmapOf(0),
                    "y", RoaringBitmap.bitmapOf(0, 1),
                    "z", RoaringBitmap.bitmapOf(1),
                    "a", RoaringBitmap.bitmapOf(0),
                    "b", RoaringBitmap.bitmapOf(0))
    );

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
         PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
         ImmutableJsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
         ImmutableJsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
         MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(jsonIndexConfig)) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }

      for (int i = 0; i < keys.length; i++) {
        Map<String, RoaringBitmap> onHeapRes = onHeapIndexReader.getMatchingFlattenedDocsMap(keys[i], null);
        onHeapIndexReader.convertFlattenedDocIdsToDocIds(onHeapRes);
        Map<String, RoaringBitmap> offHeapRes = offHeapIndexReader.getMatchingFlattenedDocsMap(keys[i], null);
        offHeapIndexReader.convertFlattenedDocIdsToDocIds(offHeapRes);
        Map<String, RoaringBitmap> mutableRes = mutableJsonIndex.getMatchingFlattenedDocsMap(keys[i], null);
        mutableJsonIndex.convertFlattenedDocIdsToDocIds(mutableRes);
        Assert.assertEquals(expected.get(i), onHeapRes);
        Assert.assertEquals(expected.get(i), offHeapRes);
        Assert.assertEquals(mutableRes, expected.get(i));
      }
    }
  }

  public static class ConfTest extends AbstractSerdeIndexContract {

    @Test
    public void oldToNewConfConversion() {
      Map<String, JsonIndexConfig> configs = new HashMap<>();
      JsonIndexConfig config = new JsonIndexConfig();
      config.setMaxLevels(2);
      configs.put("dimStr", config);
      _tableConfig.getIndexingConfig().setJsonIndexConfigs(configs);
      _tableConfig.getIndexingConfig().setJsonIndexColumns(Lists.newArrayList("dimStr2"));
      convertToUpdatedFormat();
      assertNotNull(_tableConfig.getFieldConfigList());
      assertFalse(_tableConfig.getFieldConfigList().isEmpty());
      FieldConfig fieldConfig = _tableConfig.getFieldConfigList().stream()
          .filter(fc -> fc.getName().equals("dimStr"))
          .collect(Collectors.toList()).get(0);
      assertNotNull(fieldConfig.getIndexes().get(JsonIndexType.INDEX_DISPLAY_NAME));
      assertNull(_tableConfig.getIndexingConfig().getJsonIndexColumns());
      assertNull(_tableConfig.getIndexingConfig().getJsonIndexConfigs());
    }
  }
}
