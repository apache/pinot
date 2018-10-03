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

import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.Test;


// TODO: this is an example test class. To enable the test, add setUp method and queries.
public class SumStarTreeV2Test extends BaseStarTreeV2Test<Double, Double> {

  @Test
  public void testQueries() {
    // TODO: add queries
    // testQuery("SELECT SUM(column) FROM table WHERE ... GROUP BY ...");
  }

  @Override
  protected Double getNextValue(@Nonnull BlockSingleValIterator valueIterator, @Nullable Dictionary dictionary) {
    if (dictionary == null) {
      return valueIterator.nextDoubleVal();
    } else {
      return dictionary.getDoubleValue(valueIterator.nextIntVal());
    }
  }

  @Override
  protected Double aggregate(@Nonnull List<Double> values) {
    double sum = 0;
    for (Double value : values) {
      sum += value;
    }
    return sum;
  }

  @Override
  protected void assertAggregatedValue(Double starTreeResult, Double nonStarTreeResult) {
    Assert.assertEquals(starTreeResult, nonStarTreeResult, 1e-5);
  }
}
