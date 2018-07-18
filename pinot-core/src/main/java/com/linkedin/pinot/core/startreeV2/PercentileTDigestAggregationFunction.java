/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.data.FieldSpec;
import com.clearspring.analytics.stream.quantile.TDigest;


public class PercentileTDigestAggregationFunction implements AggregationFunction<Number, TDigest> {
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getDatatype() {
    return null;
  }

  @Override
  public TDigest aggregateRaw(List<Number> data) {
    TDigest tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);

    for (Number obj: data) {
      if (obj instanceof Double) {
        tDigest.add(obj.doubleValue());
      }
    }

    return tDigest;
  }

  @Override
  public TDigest aggregatePreAggregated(List<TDigest> data) {
    TDigest tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
    for (TDigest obj: data) {
      tDigest.add(obj);
    }
    return tDigest;
  }
}
