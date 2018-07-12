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

import com.linkedin.pinot.common.data.FieldSpec;
import java.util.List;
import javax.annotation.Nonnull;


public interface AggregationFunction<R, A> {

  /**
   * Get the name of the aggregation function.
   */
  @Nonnull
  String getName();

  /**
   * Get the datatype aggregation function returns.
   */
  @Nonnull
  FieldSpec.DataType getDatatype();

  /**
   * Perform aggregation on the given raw data
   */
  A aggregateRaw(List<R> data);

  /**
   * Perform aggregation on the given specific data type data
   */
  A aggregatePreAggregated(List<A> data);

}
