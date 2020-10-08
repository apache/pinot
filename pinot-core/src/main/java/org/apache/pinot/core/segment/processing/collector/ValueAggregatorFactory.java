/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.processing.collector;

import org.apache.pinot.spi.data.FieldSpec;


/**
 * Factory class to create instances of value aggregator from the given name.
 */
public class ValueAggregatorFactory {
  public enum ValueAggregatorType {
    SUM, MAX, MIN
  }

  private ValueAggregatorFactory() {
  }

  /**
   * Construct a ValueAggregator from the given aggregator type
   */
  public static ValueAggregator getValueAggregator(String aggregatorTypeStr, FieldSpec.DataType dataType) {
    ValueAggregatorType aggregatorType = ValueAggregatorType.valueOf(aggregatorTypeStr.toUpperCase());
    switch (aggregatorType) {
      case SUM:
        return new SumValueAggregator(dataType);
      case MAX:
        return new MaxValueAggregator(dataType);
      case MIN:
        return new MinValueAggregator(dataType);
      default:
        throw new IllegalStateException("Unsupported value aggregator type : " + aggregatorTypeStr);
    }
  }
}
