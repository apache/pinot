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
package org.apache.pinot.core.operator.transform.function;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public abstract class BaseTransformFunctionTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final Random RANDOM = new Random();

  protected static final int NUM_ROWS = 1000;
  protected static final int MAX_NUM_MULTI_VALUES = 5;
  protected static final int MAX_MULTI_VALUE = 10;
  protected static final String INT_SV_COLUMN = "intSV";
  protected static final String LONG_SV_COLUMN = "longSV";
  protected static final String FLOAT_SV_COLUMN = "floatSV";
  protected static final String DOUBLE_SV_COLUMN = "doubleSV";
  protected static final String STRING_SV_COLUMN = "stringSV";
  protected static final String INT_MV_COLUMN = "intMV";
  protected static final String TIME_COLUMN = "time";

  protected final int[] _intSVValues = new int[NUM_ROWS];
  protected final long[] _longSVValues = new long[NUM_ROWS];
  protected final float[] _floatSVValues = new float[NUM_ROWS];
  protected final double[] _doubleSVValues = new double[NUM_ROWS];
  protected final String[] _stringSVValues = new String[NUM_ROWS];
  protected final int[][] _intMVValues = new int[NUM_ROWS][];
  protected final long[] _timeValues = new long[NUM_ROWS];

  protected Map<String, DataSource> _dataSourceMap;
  protected ProjectionBlock _projectionBlock;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));

    long currentTimeMs = System.currentTimeMillis();
    for (int i = 0; i < NUM_ROWS; i++) {
      _intSVValues[i] = RANDOM.nextInt();
      _longSVValues[i] = RANDOM.nextLong();
      _floatSVValues[i] = _intSVValues[i] * RANDOM.nextFloat();
      _doubleSVValues[i] = _intSVValues[i] * RANDOM.nextDouble();
      _stringSVValues[i] = Double.toString(_intSVValues[i] * RANDOM.nextDouble());

      int numValues = 1 + RANDOM.nextInt(MAX_NUM_MULTI_VALUES);
      _intMVValues[i] = new int[numValues];
      for (int j = 0; j < numValues; j++) {
        _intMVValues[i][j] = 1 + RANDOM.nextInt(MAX_MULTI_VALUE);
      }

      // Time in the past year
      _timeValues[i] = currentTimeMs - RANDOM.nextInt(365 * 24 * 3600) * 1000L;
    }

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(INT_SV_COLUMN, _intSVValues[i]);
      map.put(LONG_SV_COLUMN, _longSVValues[i]);
      map.put(FLOAT_SV_COLUMN, _floatSVValues[i]);
      map.put(DOUBLE_SV_COLUMN, _doubleSVValues[i]);
      map.put(STRING_SV_COLUMN, _stringSVValues[i]);
      map.put(INT_MV_COLUMN, ArrayUtils.toObject(_intMVValues[i]));
      map.put(TIME_COLUMN, _timeValues[i]);
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(INT_SV_COLUMN, FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec(LONG_SV_COLUMN, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(FLOAT_SV_COLUMN, FieldSpec.DataType.FLOAT, true));
    schema.addField(new DimensionFieldSpec(DOUBLE_SV_COLUMN, FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec(STRING_SV_COLUMN, FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec(INT_MV_COLUMN, FieldSpec.DataType.INT, false));
    schema.addField(new TimeFieldSpec(TIME_COLUMN, FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();

    IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);
    Set<String> columnNames = indexSegment.getPhysicalColumnNames();
    _dataSourceMap = new HashMap<>(columnNames.size());
    for (String columnName : columnNames) {
      _dataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
    }

    _projectionBlock = new ProjectionOperator(_dataSourceMap,
        new DocIdSetOperator(new MatchAllFilterOperator(NUM_ROWS), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();
  }

  protected void testTransformFunction(TransformFunction transformFunction, double[] expectedValues) {
    int[] intValues = transformFunction.transformToIntValuesSV(_projectionBlock);
    long[] longValues = transformFunction.transformToLongValuesSV(_projectionBlock);
    float[] floatValues = transformFunction.transformToFloatValuesSV(_projectionBlock);
    double[] doubleValues = transformFunction.transformToDoubleValuesSV(_projectionBlock);
    String[] stringValues = transformFunction.transformToStringValuesSV(_projectionBlock);
    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(intValues[i], (int) expectedValues[i]);
      Assert.assertEquals(longValues[i], (long) expectedValues[i]);
      Assert.assertEquals(floatValues[i], (float) expectedValues[i]);
      Assert.assertEquals(doubleValues[i], expectedValues[i]);
      Assert.assertEquals(stringValues[i], Double.toString(expectedValues[i]));
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
