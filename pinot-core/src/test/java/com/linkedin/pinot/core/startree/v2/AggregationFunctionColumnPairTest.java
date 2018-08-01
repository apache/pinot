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
package com.linkedin.pinot.core.startree.v2;

import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
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
    AggregationFunctionColumnPair fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.MIN, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MIN);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "min__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.MAX, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.MAX);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "max__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.SUM, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.SUM);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "sum__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.DISTINCTCOUNTHLL, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "distinctCountHLL__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILEEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileEst__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());

    pair = new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILETDIGEST, COLUMN);
    Assert.assertEquals(pair.getFunctionType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(pair.getColumn(), COLUMN);
    columnName = pair.toColumnName();
    Assert.assertEquals(columnName, "percentileTDigest__column");
    fromColumnName = AggregationFunctionColumnPair.fromColumnName(columnName);
    Assert.assertEquals(fromColumnName, pair);
    Assert.assertEquals(fromColumnName.hashCode(), pair.hashCode());
  }
}
