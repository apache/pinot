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

import java.util.List;
import java.io.IOException;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.Utils;
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
  public HyperLogLog aggregateRaw(List<Object> data) {
    HyperLogLog hyperLogLog = new HyperLogLog(HllConstants.DEFAULT_LOG2M);
    for (Object obj : data) {
      try {
        hyperLogLog.offer(obj);
        _maxLength = Math.max(_maxLength, hyperLogLog.getBytes().length);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return hyperLogLog;
  }

  @Override
  public HyperLogLog aggregatePreAggregated(List<HyperLogLog> data) {
    HyperLogLog hyperLogLog = data.get(0);
    for (int i = 1; i < data.size(); i++) {
      try {
        hyperLogLog.addAll(data.get(i));
        _maxLength = Math.max(_maxLength, hyperLogLog.getBytes().length);
      } catch (CardinalityMergeException e) {
        Utils.rethrowException(e);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return hyperLogLog;
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
