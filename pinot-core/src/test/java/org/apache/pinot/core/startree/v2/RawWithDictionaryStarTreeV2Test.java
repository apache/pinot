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
package org.apache.pinot.core.startree.v2;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Random;
import org.apache.pinot.segment.local.aggregator.SumValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

import static org.testng.Assert.assertEquals;


/// Exercises the full [BaseStarTreeV2Test] query matrix against star-tree dimensions configured with a `RAW`
/// forward index and a separated dictionary — the exact configuration handled by
/// [org.apache.pinot.core.startree.StarTreeUtils#toDictionaryBased] (Apache Pinot PR #19153). Both
/// `DIMENSION1` and `DIMENSION2` are switched to `RAW` + separated dictionary so every filter predicate,
/// including the `GROUP BY DIMENSION2` case, goes through the fix.
///
/// Before that fix, [org.apache.pinot.core.startree.operator.StarTreeFilterOperator] invoked
/// `getMatchingDictIds()` on a raw-value evaluator during tree traversal and threw
/// `UnsupportedOperationException`. Running the inherited `testQueries()` / `testUnsupportedFilters()`
/// suites confirms every AND/OR/NOT/nested filter combination now produces the same aggregated result
/// via the star-tree as via a plain scan.
public class RawWithDictionaryStarTreeV2Test extends BaseStarTreeV2Test<Object, Double> {

  @Override
  ValueAggregator<Object, Double> getValueAggregator() {
    return new SumValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.INT;
  }

  @Override
  Object getRandomRawValue(Random random) {
    return random.nextInt();
  }

  @Override
  protected void assertAggregatedValue(Double starTreeResult, Double nonStarTreeResult) {
    assertEquals(starTreeResult, nonStarTreeResult, 1e-5);
  }

  @Override
  protected TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawWithDictionary(DIMENSION1), rawWithDictionary(DIMENSION2)))
        .build();
  }

  /// Builds a `FieldConfig` that stores the column as a `RAW` forward index while keeping a dictionary
  /// alongside — the "separated dictionary" configuration star-tree must be able to consume.
  private static FieldConfig rawWithDictionary(String column) {
    ObjectNode indexes = JsonUtils.newObjectNode();
    ObjectNode forwardCfg = JsonUtils.newObjectNode();
    forwardCfg.put("encodingType", "RAW");
    indexes.set("forward", forwardCfg);
    ObjectNode dictionaryCfg = JsonUtils.newObjectNode();
    dictionaryCfg.put("disabled", false);
    indexes.set("dictionary", dictionaryCfg);
    return new FieldConfig.Builder(column)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
  }
}
