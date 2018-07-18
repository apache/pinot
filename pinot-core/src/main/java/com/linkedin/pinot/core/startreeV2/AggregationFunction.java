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

import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import com.linkedin.pinot.common.data.FieldSpec;


public interface AggregationFunction<R, A, S> {

  /**
   * Get the name of an aggregation function.
   */
  @Nonnull
  String getName();

  /**
   * Get the return datatype of an aggregation function.
   */
  @Nonnull
  FieldSpec.DataType getDataType();

  /**
   * Perform aggregation on the given raw data.
   */
  A aggregateRaw(List<R> data);

  /**
   * Perform aggregation on the pre aggregated data.
   */
  A aggregatePreAggregated(List<A> data);

  /**
   * Perform serialization of a object
   */
  S serialize(A obj) throws IOException;

  /**
   * Perform deserialization of a object
   */
  A deserialize(S obj) throws IOException;
}
