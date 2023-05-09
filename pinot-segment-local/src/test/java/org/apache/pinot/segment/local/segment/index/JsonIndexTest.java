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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
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
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Unit test for {@link JsonIndexCreator} and {@link JsonIndexReader}.
 */
public class JsonIndexTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIndexTest");

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
        "{" + "\"name\":\"adam\"," + "\"age\":20," + "\"addresses\":["
            + "   {\"street\":\"street-00\",\"country\":\"us\"}," + "   {\"street\":\"street-01\",\"country\":\"us\"},"
            + "   {\"street\":\"street-02\",\"country\":\"ca\"}]," + "\"skills\":[\"english\",\"programming\"]" + "}",
        "{" + "\"name\":\"bob\"," + "\"age\":25," + "\"addresses\":["
            + "   {\"street\":\"street-10\",\"country\":\"ca\"}," + "   {\"street\":\"street-11\",\"country\":\"us\"},"
            + "   {\"street\":\"street-12\",\"country\":\"in\"}]," + "\"skills\":[]" + "}",
        "{" + "\"name\":\"charles\"," + "\"age\":30," + "\"addresses\":["
            + "   {\"street\":\"street-20\",\"country\":\"jp\"}," + "   {\"street\":\"street-21\",\"country\":\"kr\"},"
            + "   {\"street\":\"street-22\",\"country\":\"cn\"}]," + "\"skills\":[\"japanese\",\"korean\",\"chinese\"]"
            + "}", "{" + "\"name\":\"david\"," + "\"age\":35," + "\"addresses\":["
        + "   {\"street\":\"street-30\",\"country\":\"ca\",\"types\":[\"home\",\"office\"]},"
        + "   {\"street\":\"street-31\",\"country\":\"ca\"}," + "   {\"street\":\"street-32\",\"country\":\"ca\"}],"
        + "\"skills\":null" + "}"
    };
    //CHECKSTYLE:ON
    // @formatter: on

    String onHeapColumnName = "onHeap";
    try (JsonIndexCreator onHeapIndexCreator = new OnHeapJsonIndexCreator(INDEX_DIR, onHeapColumnName,
        new JsonIndexConfig())) {
      for (String record : records) {
        onHeapIndexCreator.add(record);
      }
      onHeapIndexCreator.seal();
    }
    File onHeapIndexFile = new File(INDEX_DIR, onHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    String offHeapColumnName = "offHeap";
    try (JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(INDEX_DIR, offHeapColumnName,
        new JsonIndexConfig())) {
      for (String record : records) {
        offHeapIndexCreator.add(record);
      }
      offHeapIndexCreator.seal();
    }
    File offHeapIndexFile = new File(INDEX_DIR, offHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(new JsonIndexConfig())) {
      for (String record : records) {
        mutableJsonIndex.add(record);
      }
      JsonIndexReader[] indexReaders = new JsonIndexReader[]{onHeapIndexReader, offHeapIndexReader, mutableJsonIndex};
      for (JsonIndexReader indexReader : indexReaders) {
        MutableRoaringBitmap matchingDocIds = getMatchingDocIds(indexReader, "name='bob'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{1});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" = 'street-21'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].street\" NOT IN ('street-10', 'street-22')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].country\" IN ('ca', 'us')");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[1]\" = 'office'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[0].types[0]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" = 'home'");
        Assert.assertEquals(matchingDocIds.toArray(), new int[0]);

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].types[*]\" IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[1].types[*]\" IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "abc IS NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2, 3});

        matchingDocIds = getMatchingDocIds(indexReader, "\"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 2});

        matchingDocIds =
            getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'ca' AND \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0});

        matchingDocIds = getMatchingDocIds(indexReader, "\"addresses[*].country\" = 'us' OR \"skills[*]\" IS NOT NULL");
        Assert.assertEquals(matchingDocIds.toArray(), new int[]{0, 1, 2});
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

    String onHeapColumnName = "onHeap";
    try (JsonIndexCreator onHeapIndexCreator = new OnHeapJsonIndexCreator(INDEX_DIR, onHeapColumnName,
        new JsonIndexConfig())) {
      for (String record : records) {
        onHeapIndexCreator.add(record);
      }
      onHeapIndexCreator.seal();
    }
    File onHeapIndexFile = new File(INDEX_DIR, onHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(onHeapIndexFile.exists());

    String offHeapColumnName = "offHeap";
    try (JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(INDEX_DIR, offHeapColumnName,
        new JsonIndexConfig())) {
      for (String record : records) {
        offHeapIndexCreator.add(record);
      }
      offHeapIndexCreator.seal();
    }
    File offHeapIndexFile = new File(INDEX_DIR, offHeapColumnName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
    Assert.assertTrue(offHeapIndexFile.exists());

    try (PinotDataBuffer onHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(onHeapIndexFile);
        PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(offHeapIndexFile);
        JsonIndexReader onHeapIndexReader = new ImmutableJsonIndexReader(onHeapDataBuffer, records.length);
        JsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, records.length);
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(new JsonIndexConfig())) {
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

  private MutableRoaringBitmap getMatchingDocIds(JsonIndexReader indexReader, String filter) {
    return indexReader.getMatchingDocIds(filter);
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
