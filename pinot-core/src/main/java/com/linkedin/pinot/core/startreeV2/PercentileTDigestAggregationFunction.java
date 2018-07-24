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

package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import java.io.IOException;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.data.FieldSpec;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;


public class PercentileTDigestAggregationFunction implements AggregationFunction<Number, TDigest> {
  public static int _maxLength = 0;
  public final int DEFAULT_TDIGEST_COMPRESSION = 100;

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getDataType() {
    return FieldSpec.DataType.BYTES;
  }

  @Nonnull
  @Override
  public int getLongestEntrySize() {
    return _maxLength;
  }

  @Override
  public TDigest aggregateRaw(List<Number> data) {
    TDigest tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
    for (Number obj : data) {
      tDigest.add(obj.doubleValue());
      _maxLength = Math.max(tDigest.byteSize(), _maxLength);
    }
    return tDigest;
  }

  @Override
  public TDigest aggregatePreAggregated(List<TDigest> data) {
    TDigest tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
    for (TDigest obj : data) {
      tDigest.add(obj);
      _maxLength = Math.max(tDigest.byteSize(), _maxLength);
    }
    return tDigest;
  }

  @Override
  public byte[] serialize(TDigest tDigest) throws IOException {
    return ObjectCustomSerDe.serialize(tDigest);
  }

  @Override
  public TDigest deserialize(byte[] buffer) throws IOException {
    return ObjectCustomSerDe.deserialize(buffer, ObjectType.TDigest);
  }
}
