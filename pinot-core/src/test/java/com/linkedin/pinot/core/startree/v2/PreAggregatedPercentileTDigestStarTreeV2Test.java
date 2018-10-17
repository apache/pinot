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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.ObjectSerDeUtils;
import com.linkedin.pinot.core.data.aggregator.PercentileTDigestValueAggregator;
import com.linkedin.pinot.core.data.aggregator.ValueAggregator;
import com.tdunning.math.stats.TDigest;
import java.util.Random;

import static org.testng.Assert.*;


public class PreAggregatedPercentileTDigestStarTreeV2Test extends BaseStarTreeV2Test<Object, TDigest> {
  // Use non-default compression
  private static final double COMPRESSION = 50;

  @Override
  ValueAggregator<Object, TDigest> getValueAggregator() {
    return new PercentileTDigestValueAggregator();
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    TDigest tDigest = TDigest.createMergingDigest(COMPRESSION);
    tDigest.add(random.nextLong());
    tDigest.add(random.nextLong());
    return ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest);
  }

  @Override
  void assertAggregatedValue(TDigest starTreeResult, TDigest nonStarTreeResult) {
    for (int i = 0; i <= 100; i++) {
      assertEquals(starTreeResult.quantile(i / 100), nonStarTreeResult.quantile(i / 100));
    }
  }
}
