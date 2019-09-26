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
package org.apache.pinot.core.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.readers.PresenceVectorReader;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.core.segment.index.creator.RawIndexCreatorTest.getRandomValue;


/**
 * Class for testing segment generation with byte[] data type.
 */
public class SegmentGenerationWithPresenceVectorTest {
  private static final int NUM_ROWS = 10001;

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "presenceVectorTest";
  private static final String SEGMENT_NAME = "testSegment";

  private Random _random;
  private Schema _schema;
  private ImmutableSegment _segment;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";

  Map<String, boolean[]> _actualNullVectorMap = new HashMap<>();

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    _schema = new Schema();
    _schema.addField(new DimensionFieldSpec(INT_COLUMN, FieldSpec.DataType.INT, true));
    _schema.addField(new DimensionFieldSpec(LONG_COLUMN, FieldSpec.DataType.LONG, true));
    _schema.addField(new DimensionFieldSpec(FLOAT_COLUMN, FieldSpec.DataType.FLOAT, true));
    _schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, true));
    _schema.addField(new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true));

    _random = new Random(System.nanoTime());
    buildIndex(_schema);
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */

  private void buildIndex(Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      boolean[] value = new boolean[NUM_ROWS];
      Arrays.fill(value, false);
      _actualNullVectorMap.put(fieldSpec.getName(), value);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;
        value = getRandomValue(_random, fieldSpec.getDataType());
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      //Remove some values to simulate null
      int rowId = i;
      Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Object> entry = iterator.next();
        if (_random.nextDouble() < .1) {
          String key = entry.getKey();
          if (_random.nextBoolean()) {
            iterator.remove();
          } else {
            entry.setValue(null);
          }
          _actualNullVectorMap.get(key)[rowId] = true;
        }
      }
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
  }

  @Test
  public void test()
      throws Exception {
    Map<String, PresenceVectorReader> presenceVectorReaderMap = new HashMap<>();
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {

      PresenceVectorReader presenceVector = _segment.getDataSource(fieldSpec.getName()).getPresenceVector();
      System.out.println("field = " + fieldSpec.getName());
      Assert.assertNotNull(presenceVector);
      presenceVectorReaderMap.put(fieldSpec.getName(), presenceVector);
    }
    for (int i = 0; i < NUM_ROWS; i++) {
      for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
        String colName = fieldSpec.getName();
        Assert.assertEquals(_actualNullVectorMap.get(colName)[i], !presenceVectorReaderMap.get(colName).isPresent(i));
      }
    }
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup()
      throws IOException {
    _segment.destroy();
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }
}
