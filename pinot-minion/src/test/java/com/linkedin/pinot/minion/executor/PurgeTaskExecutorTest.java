/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.minion.executor;

import com.linkedin.pinot.common.config.PinotTaskConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.MinionConstants;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.minion.MinionContext;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
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
  public void setUp() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(D1, FieldSpec.DataType.INT, true));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putField(D1, i);
      rows.add(row);
    }
    GenericRowRecordReader genericRowRecordReader = new GenericRowRecordReader(rows, schema);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(ORIGINAL_SEGMENT_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, genericRowRecordReader);
    driver.build();
    _originalIndexDir = new File(ORIGINAL_SEGMENT_DIR, SEGMENT_NAME);

    MinionContext minionContext = MinionContext.getInstance();
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
          row.putField(D1, Integer.MAX_VALUE);
          return true;
        };
      } else {
        return null;
      }
    });
  }

  @Test
  public void testConvert() throws Exception {
    PurgeTaskExecutor purgeTaskExecutor = new PurgeTaskExecutor();
    PinotTaskConfig pinotTaskConfig = new PinotTaskConfig(MinionConstants.PurgeTask.TASK_TYPE,
        Collections.singletonMap(MinionConstants.TABLE_NAME_KEY,
            TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME)));
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
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}