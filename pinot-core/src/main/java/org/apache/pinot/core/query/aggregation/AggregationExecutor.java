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
package org.apache.pinot.core.query.aggregation;

import java.util.List;
import org.apache.pinot.core.operator.blocks.TransformBlock;


/**
 * Interface for Aggregation executor, that executes all aggregation functions (without group-bys).
 * <p>Aggregations are performed within a segment, i.e. does not merge aggregation results across different segments.
 */
public interface AggregationExecutor {

  /**
   * Performs aggregation on the given transform block.
   *
   * @param transformBlock Transform Block
   */
  void aggregate(TransformBlock transformBlock);

  /**
   * Returns the result of aggregation.
   * <p>Should be called after all transform blocks has been aggregated.
   *
   * @return Result of aggregation
   */
  List<Object> getResult();
}
