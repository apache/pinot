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

import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
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
 * Tests for PERCENTILE_TDIGEST and PERCENTILE_TDIGEST_MV aggregation functions.
 *
 * <ul>
 *   <li>Generates a segment with a double multi-valued column, a TDigest column and a group-by column</li>
 *   <li>Runs aggregation and group-by queries on the generated segment</li>
 *   <li>
 *     Compares the results for PERCENTILE_TDIGEST_MV on double multi-valued column, PERCENTILE_TDIGEST on TDigest
 *     column with results for PERCENTILE_MV on double multi-valued column
 *   </li>
 * </ul>
 */
public class PercentileTDigestMVQueriesTest extends PercentileTDigestQueriesTest {
  private static final int MAX_NUM_MULTI_VALUES = 10;

  @Override
  protected void buildSegment()
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> valueMap = new HashMap<>();

      int numMultiValues = RANDOM.nextInt(MAX_NUM_MULTI_VALUES) + 1;
      Double[] values = new Double[numMultiValues];
      TDigest tDigest = TDigest.createMergingDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      for (int j = 0; j < numMultiValues; j++) {
        double value = RANDOM.nextDouble() * VALUE_RANGE;
        values[j] = value;
        tDigest.add(value);
      }
      valueMap.put(DOUBLE_COLUMN, values);
      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      valueMap.put(TDIGEST_COLUMN, byteBuffer.array());

      String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
      valueMap.put(GROUP_BY_COLUMN, group);

      GenericRow genericRow = new GenericRow();
      genericRow.init(valueMap);
      rows.add(genericRow);
    }

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, false));
    schema.addField(new MetricFieldSpec(TDIGEST_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Collections.singletonList(TDIGEST_COLUMN));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Override
  protected String getAggregationQuery(int percentile) {
    return String.format("SELECT PERCENTILE%1$dMV(%2$s), PERCENTILETDIGEST%1$dMV(%2$s), PERCENTILETDIGEST%1$d(%3$s), "
            + "PERCENTILEMV(%2$s, %1$d), PERCENTILETDIGESTMV(%2$s, %1$d), PERCENTILETDIGEST(%3$s, %1$d) FROM %4$s",
        percentile, DOUBLE_COLUMN, TDIGEST_COLUMN, TABLE_NAME);
  }
}
