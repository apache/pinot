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
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.core.query.aggregation.function.PercentileTDigestAggregationFunction;
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
 * Tests for PERCENTILE_TDIGEST and PERCENTILE_TDIGEST_MV aggregation functions.
 *
 * <ul>
 *   <li>
 *     Generates a segment with a double multi-valued column, a TDigest column, a custom compression TDigest column
 *     and a group-by column
 *   </li>
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
      GenericRow row = new GenericRow();

      int numMultiValues = RANDOM.nextInt(MAX_NUM_MULTI_VALUES) + 1;
      Double[] values = new Double[numMultiValues];
      TDigest tDigest = TDigest.createMergingDigest(PercentileTDigestAggregationFunction.DEFAULT_TDIGEST_COMPRESSION);
      TDigest tDigestCustom = TDigest.createMergingDigest(CUSTOM_COMPRESSION);
      for (int j = 0; j < numMultiValues; j++) {
        double value = RANDOM.nextDouble() * VALUE_RANGE;
        values[j] = value;
        tDigest.add(value);
        tDigestCustom.add(value);
      }
      row.putValue(DOUBLE_COLUMN, values);

      ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.byteSize());
      tDigest.asBytes(byteBuffer);
      row.putValue(TDIGEST_COLUMN, byteBuffer.array());

      ByteBuffer byteBufferCustom = ByteBuffer.allocate(tDigestCustom.byteSize());
      tDigestCustom.asBytes(byteBufferCustom);
      row.putValue(TDIGEST_CUSTOM_COMPRESSION_COLUMN, byteBufferCustom.array());

      String group = GROUPS[RANDOM.nextInt(GROUPS.length)];
      row.putValue(GROUP_BY_COLUMN, group);

      rows.add(row);
    }

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, false));
    schema.addField(new MetricFieldSpec(TDIGEST_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new MetricFieldSpec(TDIGEST_CUSTOM_COMPRESSION_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new DimensionFieldSpec(GROUP_BY_COLUMN, FieldSpec.DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Arrays.asList(TDIGEST_COLUMN, TDIGEST_CUSTOM_COMPRESSION_COLUMN));

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Override
  protected String getAggregationQuery(int percentile) {
    return String.format("SELECT PERCENTILE%1$dMV(%2$s), PERCENTILETDIGEST%1$dMV(%2$s), PERCENTILETDIGEST%1$d(%3$s), "
            + "PERCENTILEMV(%2$s, %1$d), PERCENTILETDIGESTMV(%2$s, %1$d), PERCENTILETDIGEST(%3$s, %1$d), "
            + "PERCENTILETDIGESTMV(%2$s, %1$d, %6$d), PERCENTILETDIGEST(%5$s, %1$d, %6$d) FROM %4$s",
        percentile, DOUBLE_COLUMN, TDIGEST_COLUMN, TABLE_NAME, TDIGEST_CUSTOM_COMPRESSION_COLUMN, CUSTOM_COMPRESSION);
  }

  @Override
  protected String getSmartTDigestQuery(int percentile) {
    return String.format("SELECT PERCENTILEMV(%2$s, %1$d), PERCENTILESMARTTDIGEST(%2$s, %1$d), "
            + "PERCENTILETDIGESTMV(%2$s, %1$d), PERCENTILESMARTTDIGEST(%2$s, %1$d, 'threshold=10') FROM %3$s",
        percentile,
        DOUBLE_COLUMN, TABLE_NAME);
  }
}
