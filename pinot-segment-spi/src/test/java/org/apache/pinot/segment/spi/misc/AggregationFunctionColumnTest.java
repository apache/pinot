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
package org.apache.pinot.segment.spi.misc;

import java.util.Map;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumn;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionColumnTest {
  private static final String COLUMN = "column";

  @Test
  public void testToAndFromColumnName() {
    AggregationFunctionColumn pair = new AggregationFunctionColumn(AggregationFunctionType.COUNT, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.COUNT);
    Assert.assertEquals(pair.getColumn(), AggregationFunctionColumn.STAR);
    String columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "count__*");
    AggregationFunctionColumn fromColumnName = AggregationFunctionColumn.fromColumnName("count__*");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.MIN, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MIN);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "min__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("MIN__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.MAX, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MAX);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "max__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("mAx__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.SUM, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.SUM);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "sum__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("SuM__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "distinctCountHLL__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("distinctCountHLL__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTRAWHLL, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "distinctCountRawHLL__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("distinct_count_raw_hll__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.PERCENTILEEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileEst__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("PERCENTILE_EST__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.PERCENTILERAWEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILERAWEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileRawEst__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("PERCENTILE_RAW_EST__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.PERCENTILETDIGEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileTDigest__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("percentiletdigest__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumn(AggregationFunctionType.PERCENTILERAWTDIGEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileRawTDigest__column");
    fromColumnName = AggregationFunctionColumn.fromColumnName("percentilerawtdigest__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());
  }

  @Test
  public void testEqualityWithConfiguration() {
    // DISTINCTCOUNTHLL - log2m value should be equal (null config implies default log2m value of 8)
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN, Map.of())
    );
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN, Map.of()),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN,
            Map.of(Constants.HLL_LOG2M_KEY, 8))
    );
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN,
            Map.of(Constants.HLL_LOG2M_KEY, "8")),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN, Map.of())
    );
    Assert.assertNotEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN, Map.of()),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN,
            Map.of(Constants.HLL_LOG2M_KEY, 16))
    );
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN,
            Map.of(Constants.HLL_LOG2M_KEY, "16")),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN,
            Map.of(Constants.HLL_LOG2M_KEY, 16))
    );

    // DISTINCTCOUNTHLLPLUS - p value should be equal (null config implies default p value of 14); sp value not
    // considered
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN, Map.of()),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN)
    );
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN,
            Map.of(Constants.HLLPLUS_ULL_P_KEY, 14, Constants.HLLPLUS_SP_KEY, 0)),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN, Map.of())
    );
    Assert.assertNotEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN,
            Map.of(Constants.HLLPLUS_ULL_P_KEY, 4, Constants.HLLPLUS_SP_KEY, 32)),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN, Map.of())
    );
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN,
            Map.of(Constants.HLLPLUS_ULL_P_KEY, "4", Constants.HLLPLUS_SP_KEY, "32")),
        new AggregationFunctionColumn(AggregationFunctionType.DISTINCTCOUNTHLLPLUS, COLUMN,
            Map.of(Constants.HLLPLUS_ULL_P_KEY, 4, Constants.HLLPLUS_SP_KEY, 30))
    );

    // PERCENTILETDIGEST - any configuration is considered equal
    Assert.assertEquals(
        new AggregationFunctionColumn(AggregationFunctionType.PERCENTILETDIGEST, COLUMN,
            Map.of("percentile", 99, Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 200)),
        new AggregationFunctionColumn(AggregationFunctionType.PERCENTILETDIGEST, COLUMN,
            Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100"))
    );
  }
}
