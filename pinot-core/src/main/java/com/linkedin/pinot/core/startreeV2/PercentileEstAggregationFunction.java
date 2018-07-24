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
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;


public class PercentileEstAggregationFunction implements AggregationFunction<Number, QuantileDigest> {

  public static int _maxLength = 0;
  public final double DEFAULT_MAX_ERROR = 0.05;

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.PERCENTILEEST;
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
  public QuantileDigest aggregateRaw(List<Number> data) {
    QuantileDigest qDigest = new QuantileDigest(DEFAULT_MAX_ERROR);

    for (Number obj : data) {
      qDigest.add(obj.longValue());
      _maxLength = Math.max(qDigest.estimatedSerializedSizeInBytes(), _maxLength);
    }
    return qDigest;
  }

  @Override
  public QuantileDigest aggregatePreAggregated(List<QuantileDigest> data) {
    QuantileDigest qDigest = new QuantileDigest(DEFAULT_MAX_ERROR);
    for (QuantileDigest obj : data) {
      qDigest.merge(obj);
      _maxLength = Math.max(qDigest.estimatedSerializedSizeInBytes(), _maxLength);
    }
    return qDigest;
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
