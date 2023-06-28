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
package org.apache.pinot.core.minion;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class SegmentPurgerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentPurgerTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File PURGED_SEGMENT_DIR = new File(TEMP_DIR, "purgedSegment");
  private static final Random RANDOM = new Random();

  private static final int NUM_ROWS = 10000;
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String D1 = "d1";
  private static final String D2 = "d2";

  private TableConfig _tableConfig;
  private Schema _schema;
  private File _originalIndexDir;
  private int _expectedNumRecordsPurged;
  private int _expectedNumRecordsModified;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Collections.singletonList(D1)).setCreateInvertedIndexDuringSegmentGeneration(true)
        .build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT)
        .addSingleValueDimension(D2, FieldSpec.DataType.INT).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      int value1 = RANDOM.nextInt(100);
      int value2 = RANDOM.nextInt(100);
      if (value1 == 0) {
        _expectedNumRecordsPurged++;
      } else if (value2 == 0) {
        _expectedNumRecordsModified++;
      }
      row.putValue(D1, value1);
      row.putValue(D2, value2);
      rows.add(row);
    }
    GenericRowRecordReader genericRowRecordReader = new GenericRowRecordReader(rows);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, genericRowRecordReader);
    driver.build();
    _originalIndexDir = new File(ORIGINAL_SEGMENT_DIR, SEGMENT_NAME);
  }

  @Test
  public void testPurgeSegment()
      throws Exception {
    // Purge records with d1 = 0
    SegmentPurger.RecordPurger recordPurger = row -> row.getValue(D1).equals(0);

    // Modify records with d2 = 0 to d2 = Integer.MAX_VALUE
    SegmentPurger.RecordModifier recordModifier = row -> {
      if (row.getValue(D2).equals(0)) {
        row.putValue(D2, Integer.MAX_VALUE);
        return true;
      } else {
        return false;
      }
    };

    SegmentPurger segmentPurger =
        new SegmentPurger(_originalIndexDir, PURGED_SEGMENT_DIR, _tableConfig, _schema, recordPurger, recordModifier);
    File purgedIndexDir = segmentPurger.purgeSegment();

    // Check the purge/modify counter in segment purger
    assertEquals(segmentPurger.getNumRecordsPurged(), _expectedNumRecordsPurged);
    assertEquals(segmentPurger.getNumRecordsModified(), _expectedNumRecordsModified);

    // Check crc and index creation time
    SegmentMetadataImpl purgedSegmentMetadata = new SegmentMetadataImpl(purgedIndexDir);
    SegmentMetadataImpl originalSegmentMetadata = new SegmentMetadataImpl(_originalIndexDir);
    assertNotEquals(purgedSegmentMetadata.getCrc(), originalSegmentMetadata.getCrc());
    assertEquals(purgedSegmentMetadata.getIndexCreationTime(), originalSegmentMetadata.getIndexCreationTime());

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(purgedIndexDir)) {
      int numRecordsRemaining = 0;
      int numRecordsModified = 0;

      GenericRow row = new GenericRow();
      while (pinotSegmentRecordReader.hasNext()) {
        row = pinotSegmentRecordReader.next(row);

        // Purged segment should not have any record with d1 = 0 or d2 = 0
        assertNotEquals(row.getValue(D1), 0);
        assertNotEquals(row.getValue(D2), 0);

        numRecordsRemaining++;
        if (row.getValue(D2).equals(Integer.MAX_VALUE)) {
          numRecordsModified++;
        }
      }

      assertEquals(numRecordsRemaining, NUM_ROWS - _expectedNumRecordsPurged);
      assertEquals(numRecordsModified, _expectedNumRecordsModified);
    }

    // Check inverted index
    Map<String, Object> props = new HashMap<>();
    props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap.toString());
    try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
        .load(purgedIndexDir.toURI(), new SegmentDirectoryLoaderContext.Builder().setTableConfig(_tableConfig)
            .setSegmentName(purgedSegmentMetadata.getName()).setSegmentDirectoryConfigs(new PinotConfiguration(props))
            .build()); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(reader.hasIndexFor(D1, StandardIndexes.inverted()));
      assertFalse(reader.hasIndexFor(D2, StandardIndexes.inverted()));
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
