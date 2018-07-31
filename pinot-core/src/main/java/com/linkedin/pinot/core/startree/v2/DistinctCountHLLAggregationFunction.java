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
import com.linkedin.pinot.startree.hll.HllConstants;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;


public class DistinctCountHLLAggregationFunction implements AggregationFunction<Object, HyperLogLog> {

  public static int _maxLength = 0;

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getDataType() {
    return FieldSpec.DataType.BYTES;
  }

  @Nonnull
  @Override
  public int getResultMaxByteSize() {
    return _maxLength;
  }

  @Override
  public HyperLogLog convert(Object data) {
    HyperLogLog hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    hyperLogLog.offer(data);
    try {
      _maxLength = Math.max(_maxLength, hyperLogLog.getBytes().length);
    } catch (IOException e) {
      e.printStackTrace();
    }

    return hyperLogLog;
  }

  @Override
  public HyperLogLog aggregate(HyperLogLog obj1, HyperLogLog obj2) {
    try {
      obj1.addAll(obj2);
      _maxLength = Math.max(_maxLength, obj1.getBytes().length);
    } catch (CardinalityMergeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return obj1;
  }

  @Override
  public byte[] serialize(HyperLogLog hyperLogLog) throws IOException {
    return ObjectCustomSerDe.serialize(hyperLogLog);
  }

  @Override
  public HyperLogLog deserialize(byte[] buffer) throws IOException {
    return ObjectCustomSerDe.deserialize(buffer, ObjectType.HyperLogLog);
  }
}
