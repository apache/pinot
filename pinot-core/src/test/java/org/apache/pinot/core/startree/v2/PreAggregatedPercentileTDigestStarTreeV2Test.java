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
import java.util.Random;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.local.aggregator.PercentileTDigestValueAggregator;
import org.apache.pinot.segment.local.aggregator.ValueAggregator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import static org.testng.Assert.assertEquals;


public class PreAggregatedPercentileTDigestStarTreeV2Test extends BaseStarTreeV2Test<Object, TDigest> {
  // Use non-default compression
  private static final double COMPRESSION = 50;
  private static final int MAX_VALUE = 10000;

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
    tDigest.add(random.nextInt(MAX_VALUE));
    tDigest.add(random.nextInt(MAX_VALUE));
    return ObjectSerDeUtils.TDIGEST_SER_DE.serialize(tDigest);
  }

  @Override
  void assertAggregatedValue(TDigest starTreeResult, TDigest nonStarTreeResult) {
    double delta = MAX_VALUE * 0.05;
    for (int i = 0; i <= 100; i++) {
      assertEquals(starTreeResult.quantile(i / 100.0), nonStarTreeResult.quantile(i / 100.0), delta);
    }
  }

  @Override
  protected FieldConfig.CompressionCodec getCompressionCodec() {
    return FieldConfig.CompressionCodec.PASS_THROUGH;
  }
}
