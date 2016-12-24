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
package com.linkedin.pinot.core.segment.index.creator;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.TestRecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.io.compression.ChunkCompressorFactory;
import com.linkedin.pinot.core.io.compression.ChunkDecompressor;
import com.linkedin.pinot.core.io.reader.impl.VarByteReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Class for testing forward index creators.
 */
public class ForwardIndexCreatorTest {

  private static final int NUM_ROWS = 1009;
  private static final int MAX_STRING_LENGTH = 101;

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "fwdIndexTest";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String COLUMN_NAME = "rawIndexColumn";

  /**
   * Test for raw index creator.
   * <ul>
   *   <li> Creates a segment with one single-value string column. Uses raw index (no dictionary) for it. </li>
   *   <li> Readers the index file, asserting that values written are as expected. </li>
   * </ul>
   * @throws Exception
   */
  @Test
  public void testRawIndexCreator()
      throws Exception {
    String[] expected = buildIndex();

    File segmentDir = new File(SEGMENT_DIR_NAME);
    File indexFile = new File(SEGMENT_DIR_NAME, File.separator + SEGMENT_NAME + File.separator + COLUMN_NAME
        + V1Constants.Indexes.RAW_SV_FWD_IDX_FILE_EXTENTION);
    Assert.assertTrue(indexFile.exists(), "Index file not generated: " + indexFile.getAbsolutePath());

    ChunkDecompressor uncompressor = ChunkCompressorFactory.getDecompressor("snappy");
    VarByteSingleValueReader reader = new VarByteSingleValueReader(
        PinotDataBuffer.fromFile(indexFile, ReadMode.heap, FileChannel.MapMode.READ_ONLY, getClass().getName()),
        uncompressor);

    VarByteReaderContext context = reader.createContext();
    for (int i = 0; i < NUM_ROWS; i++) {
      String actual = reader.getString(i, context);
      Assert.assertEquals(actual, expected[i]);
    }

    FileUtils.deleteQuietly(segmentDir);
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */
  private String[] buildIndex()
      throws Exception {
    Schema schema = new Schema();

    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(dimensionFieldSpec);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(Collections.singletonList(COLUMN_NAME));

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    String[] expected = new String[NUM_ROWS];
    final List<GenericRow> rows = new ArrayList<>();
    Random random = new Random();

    for (int row = 0; row < NUM_ROWS; row++) {
      HashMap<String, Object> map = new HashMap<>();
      String value =
          (row == 0) ? "" : StringUtil.trimTrailingNulls(RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH)));

      expected[row] = value;
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader reader = new TestRecordReader(rows, schema);
    driver.init(config, reader);
    driver.build();

    return expected;
  }
}
