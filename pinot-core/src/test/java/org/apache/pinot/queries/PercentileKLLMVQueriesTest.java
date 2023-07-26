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
package org.apache.pinot.queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Variation of the PercentileKLLQueriesTest suite which tests PERCENTILE_KLL_MV
 */
public class PercentileKLLMVQueriesTest extends PercentileKLLQueriesTest {
  private static final int MAX_NUM_MULTI_VALUES = 10;

  @Override
  protected void buildSegment()
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();

      int numMultiValues = RANDOM.nextInt(MAX_NUM_MULTI_VALUES) + 1;
      Double[] values = new Double[numMultiValues];
      KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
      for (int j = 0; j < numMultiValues; j++) {
        double value = RANDOM.nextDouble() * VALUE_RANGE;
        values[j] = value;
        sketch.update(value);
      }
      row.putValue(DOUBLE_COLUMN, values);
      row.putValue(KLL_COLUMN, sketch.toByteArray());

      String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
      row.putValue(GROUP_BY_COLUMN, group);

      rows.add(row);
    }

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, false));
    schema.addField(new MetricFieldSpec(KLL_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Collections.singletonList(KLL_COLUMN));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Override
  protected String getAggregationQuery(int percentile) {
    return String.format(
        "SELECT PERCENTILEMV(%2$s, %1$d), PERCENTILEKLLMV(%2$s, %1$d), PERCENTILEKLL(%3$s, %1$d) FROM %4$s", percentile,
        DOUBLE_COLUMN, KLL_COLUMN, TABLE_NAME);
  }
}
