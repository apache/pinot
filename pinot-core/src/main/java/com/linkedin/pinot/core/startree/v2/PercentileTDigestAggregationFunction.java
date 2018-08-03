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

import java.io.IOException;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.data.FieldSpec;
import com.clearspring.analytics.stream.quantile.TDigest;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;


public class PercentileTDigestAggregationFunction implements AggregationFunction<Number, TDigest> {
  protected int _maxLength = 0;
  public final int DEFAULT_TDIGEST_COMPRESSION = 100;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getResultDataType() {
    return FieldSpec.DataType.BYTES;
  }

  @Override
  public int getResultMaxByteSize() {
    return _maxLength;
  }

  @Override
  public TDigest convert(Number data) {
    TDigest tDigest = new TDigest(DEFAULT_TDIGEST_COMPRESSION);
    tDigest.add(data.doubleValue());
    _maxLength = Math.max(tDigest.byteSize(), _maxLength);

    return tDigest;
  }

  @Override
  public TDigest aggregate(TDigest obj1, TDigest obj2) {
    obj1.add(obj2);
    _maxLength = Math.max(obj1.byteSize(), _maxLength);

    return obj1;
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
