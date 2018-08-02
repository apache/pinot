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


public class SumAggregationFunction implements AggregationFunction<Number, Double> {

  @Nonnull
  @Override
  public String getName() {
    return StarTreeV2Constant.AggregateFunctions.SUM;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getDataType() {
    return FieldSpec.DataType.DOUBLE;
  }

  @Override
  public int getResultMaxByteSize() {
    return Double.BYTES;
  }

  @Override
  public Double convert(Number data) {
    return data.doubleValue();
  }

  @Override
  public Double aggregate(Double obj1, Double obj2) {
    return obj1 + obj2;
  }

  @Override
  public byte[] serialize(Double obj) throws IOException {
    throw new IOException("method Not Supported");
  }

  @Override
  public Double deserialize(byte[] obj) throws IOException {
    throw new IOException("method Not Supported");
  }
}
