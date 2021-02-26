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
package org.apache.pinot.plugin.minion.executor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.minion.rollup.MergeType;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class MergeRollupTaskExecutorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MergeRollupTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File WORKING_DIR = new File(TEMP_DIR, "workingDir");
  private static final int NUM_SEGMENTS = 10;
  private static final int NUM_ROWS = 5;
  private static final String MERGED_SEGMENT_NAME = "testMergedSegment";
  private static final String TABLE_NAME = "testTable";
  private static final String D1 = "d1";

  private List<File> _segmentIndexDirList;

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

    _segmentIndexDirList = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      String segmentName = MERGED_SEGMENT_NAME + i;
      RecordReader recordReader = new GenericRowRecordReader(rows);
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
      config.setTableName(TABLE_NAME);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
      _segmentIndexDirList.add(new File(ORIGINAL_SEGMENT_DIR, segmentName));
    }

    MinionContext minionContext = MinionContext.getInstance();
    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> helixPropertyStore = Mockito.mock(ZkHelixPropertyStore.class);
    Mockito.when(helixPropertyStore.get("/CONFIGS/TABLE/testTable_OFFLINE", null, AccessOption.PERSISTENT))
        .thenReturn(TableConfigUtils.toZNRecord(tableConfig));
    minionContext.setHelixPropertyStore(helixPropertyStore);
  }

  @Test
  public void testConvert()
      throws Exception {
    MergeRollupTaskExecutor mergeRollupTaskExecutor = new MergeRollupTaskExecutor();
    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.MergeRollupTask.MERGE_TYPE_KEY, MergeType.CONCATENATE.toString());
    configs.put(MinionConstants.MergeRollupTask.MERGED_SEGMENT_NAME_KEY, MERGED_SEGMENT_NAME);
    configs.put(MinionConstants.TABLE_NAME_KEY, "testTable_OFFLINE");

    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs);
    List<SegmentConversionResult> conversionResults =
        mergeRollupTaskExecutor.convert(pinotTaskConfig, _segmentIndexDirList, WORKING_DIR);

    Assert.assertEquals(conversionResults.size(), 1);
    Assert.assertEquals(conversionResults.get(0).getSegmentName(), MERGED_SEGMENT_NAME);
    File mergedSegment = conversionResults.get(0).getFile();
    try (PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(mergedSegment)) {
      int numRecords = 0;
      GenericRow row = new GenericRow();
      while (pinotSegmentRecordReader.hasNext()) {
        row = pinotSegmentRecordReader.next(row);
        numRecords++;
      }
      Assert.assertEquals(numRecords, NUM_SEGMENTS * NUM_ROWS);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
