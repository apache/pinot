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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import java.io.IOException;
import javax.annotation.Nonnull;


public class PercentileEstAggregationFunction implements AggregationFunction<Number, QuantileDigest> {

  protected int _maxLength = 0;
  public final double DEFAULT_MAX_ERROR = 0.05;

  @Nonnull
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.PERCENTILEEST;
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
  public QuantileDigest convert(Number data) {
    QuantileDigest qDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
    qDigest.add(data.longValue());
    _maxLength = Math.max(qDigest.estimatedSerializedSizeInBytes(), _maxLength);

    return qDigest;
  }

  @Override
  public QuantileDigest aggregate(QuantileDigest obj1, QuantileDigest obj2) {
    obj1.merge(obj2);
    _maxLength = Math.max(obj1.estimatedSerializedSizeInBytes(), _maxLength);

    return obj1;
  }

  @Override
  public byte[] serialize(QuantileDigest qDigest) throws IOException {
    return ObjectCustomSerDe.serialize(qDigest);
  }

  @Override
  public QuantileDigest deserialize(byte[] buffer) throws IOException {
    return ObjectCustomSerDe.deserialize(buffer, ObjectType.QuantileDigest);
  }
}
