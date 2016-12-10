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

package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionVisitorBase;
import com.linkedin.pinot.core.operator.aggregation.function.FastHllAggregationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// class is public because existing tests are in different package
public class AggregationFunctionInitializer extends AggregationFunctionVisitorBase {
  private static Logger LOGGER = LoggerFactory.getLogger(AggregationFunctionInitializer.class);

  SegmentMetadata metadata;

  public AggregationFunctionInitializer(SegmentMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public void visit(FastHllAggregationFunction function) {
    function.init(metadata.getHllLog2m());
  }
}
