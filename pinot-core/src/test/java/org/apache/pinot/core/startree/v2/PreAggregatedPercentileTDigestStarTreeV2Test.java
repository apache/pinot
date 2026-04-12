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

import com.tdunning.math.stats.TDigest;
import java.util.Collections;
import java.util.Random;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.aggregator.PercentileTDigestValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class PreAggregatedPercentileTDigestStarTreeV2Test extends BaseStarTreeV2Test<Object, TDigest> {
  // Use high compression to keep star-tree vs non-star-tree quantile divergence within 0.5%.
  // t-digest 3.3 changed centroid management (unit-weight first/last centroids, stricter tail interpolation),
  // which increases merge-order sensitivity. The star-tree path does multi-level serialize/deserialize/merge
  // while the non-star-tree path merges sequentially, causing quantile divergence at low compression values.
  // Experimentally verified: compression >= 750 keeps error < 0.5% across 10 randomized runs.
  private static final double COMPRESSION = 750;
  private static final double MAX_ERROR = 0.005;
  private static final int MAX_VALUE = 10000;

  @Override
  ValueAggregator<Object, TDigest> getValueAggregator() {
    return new PercentileTDigestValueAggregator(Collections.emptyList());
  }

  @Override
  DataType getRawValueType() {
    return DataType.BYTES;
  }

  @Override
  Object getRandomRawValue(Random random) {
    TDigest tDigest = TDigest.createMergingDigest(COMPRESSION);
    tDigest.add(random.nextInt(MAX_VALUE));
    tDigest.add(random.nextInt(MAX_VALUE));
    return ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest);
  }

  @Override
  void assertAggregatedValue(TDigest starTreeResult, TDigest nonStarTreeResult) {
    double delta = MAX_VALUE * MAX_ERROR;
    for (int i = 0; i <= 100; i++) {
      assertEquals(starTreeResult.quantile(i / 100.0), nonStarTreeResult.quantile(i / 100.0), delta);
    }
  }
}
