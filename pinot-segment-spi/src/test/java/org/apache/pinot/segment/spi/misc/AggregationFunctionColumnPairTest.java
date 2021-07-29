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

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionColumnPairTest {
  private static final String COLUMN = "column";

  @Test
  public void testToAndFromColumnName() {
    AggregationFunctionColumnPair pair = new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.COUNT);
    Assert.assertEquals(pair.getColumn(), AggregationFunctionColumnPair.STAR);
    String columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "count__*");
    AggregationFunctionColumnPair fromColumnName = AggregationFunctionColumnPair.fromColumnName("count__*");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.MIN, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MIN);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "min__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("MIN__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.MAX, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MAX);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "max__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("mAx__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.SUM, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.SUM);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "sum__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("SuM__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "distinctCountHLL__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("distinctCountHLL__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTRAWHLL, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "distinctCountRawHLL__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("distinct_count_raw_hll__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILEEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileEst__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("PERCENTILE_EST__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILERAWEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILERAWEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileRawEst__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("PERCENTILE_RAW_EST__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILETDIGEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileTDigest__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("percentiletdigest__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILERAWTDIGEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileRawTDigest__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName("percentilerawtdigest__column");
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());
  }
}
