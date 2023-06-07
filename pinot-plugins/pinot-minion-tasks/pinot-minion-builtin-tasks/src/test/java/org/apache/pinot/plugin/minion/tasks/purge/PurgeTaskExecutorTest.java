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
package org.apache.pinot.plugin.minion.tasks.purge;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.event.MinionProgressObserver;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This task only tests the basic functionality of {@link PurgeTaskExecutor#convert(PinotTaskConfig, File, File)}.
 * Random test for segment purger is covered in SegmentPurgerTest.
 */
public class PurgeTaskExecutorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PurgeTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File PURGED_SEGMENT_DIR = new File(TEMP_DIR, "purgedSegment");

  private static final int NUM_ROWS = 5;
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String D1 = "d1";

  private File _originalIndexDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(D1, FieldSpec.DataType.INT).build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(D1, i);
      rows.add(row);
    }
    GenericRowRecordReader genericRowRecordReader = new GenericRowRecordReader(rows);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, genericRowRecordReader);
    driver.build();
    _originalIndexDir = new File(ORIGINAL_SEGMENT_DIR, SEGMENT_NAME);

    MinionContext minionContext = MinionContext.getInstance();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(helixPropertyStore.get("/CONFIGS/TABLE/testTable_OFFLINE", null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfig));
    Mockito.when(helixPropertyStore.get("/SCHEMAS/testTable", null, AccessOption.PERSISTENT))
        .thenReturn(SchemaUtils.toZNRecord(schema));
    minionContext.setHelixPropertyStore(helixPropertyStore);
    minionContext.setRecordPurgerFactory(rawTableName -> {
      if (rawTableName.equals(TABLE_NAME)) {
        return row -> row.getValue(D1).equals(0);
      } else {
        return null;
      }
    });
    minionContext.setRecordModifierFactory(rawTableName -> {
      if (rawTableName.equals(TABLE_NAME)) {
        return row -> {
          row.putValue(D1, Integer.MAX_VALUE);
          return true;
        };
      } else {
        return null;
      }
    });
  }

  @Test
  public void testConvert()
      throws Exception {
    PurgeTaskExecutor purgeTaskExecutor = new PurgeTaskExecutor();
    purgeTaskExecutor.setMinionEventObserver(new MinionProgressObserver());
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.PurgeTask.TASK_TYPE, Collections
        .singletonMap(MinionConstants.TABLE_NAME_KEY, TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME)));
    File purgedIndexDir = purgeTaskExecutor.convert(pinotTaskConfig, _originalIndexDir, PURGED_SEGMENT_DIR).getFile();

    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(purgedIndexDir)) {
      int numRecordsRemaining = 0;
      int numRecordsModified = 0;

      GenericRow row = new GenericRow();
      while (pinotSegmentRecordReader.hasNext()) {
        row = pinotSegmentRecordReader.next(row);
        numRecordsRemaining++;
        if (row.getValue(D1).equals(Integer.MAX_VALUE)) {
          numRecordsModified++;
        }
      }

      Assert.assertEquals(numRecordsRemaining, NUM_ROWS - 1);
      Assert.assertEquals(numRecordsModified, NUM_ROWS - 1);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
