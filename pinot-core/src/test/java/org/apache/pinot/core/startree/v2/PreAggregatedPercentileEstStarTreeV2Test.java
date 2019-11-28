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

import java.util.Random;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.data.aggregator.PercentileEstValueAggregator;
import org.apache.pinot.core.data.aggregator.ValueAggregator;
import org.apache.pinot.core.query.aggregation.function.customobject.QuantileDigest;

import static org.testng.Assert.assertEquals;


public class PreAggregatedPercentileEstStarTreeV2Test extends BaseStarTreeV2Test<Object, QuantileDigest> {
  // Use non-default max error
  private static final double MAX_ERROR = 0.1;

  @Override
  ValueAggregator<Object, QuantileDigest> getValueAggregator() {
    return new PercentileEstValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    QuantileDigest quantileDigest = new QuantileDigest(MAX_ERROR);
    quantileDigest.add(random.nextLong());
    quantileDigest.add(random.nextLong());
    return ObjectSerDeUtils.QUANTILE_DIGEST_SER_DE.serialize(quantileDigest);
  }

  @Override
  void assertAggregatedValue(QuantileDigest starTreeResult, QuantileDigest nonStarTreeResult) {
    double delta = Long.MAX_VALUE * MAX_ERROR * 2;
    for (int i = 0; i <= 100; i++) {
      assertEquals(starTreeResult.getQuantile(i / 100), nonStarTreeResult.getQuantile(i / 100), delta);
    }
  }
}
