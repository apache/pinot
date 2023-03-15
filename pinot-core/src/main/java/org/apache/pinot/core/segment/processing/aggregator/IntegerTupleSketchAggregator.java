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
package org.apache.pinot.core.segment.processing.aggregator;

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;
import org.apache.pinot.core.common.ObjectSerDeUtils;


public class IntegerTupleSketchAggregator implements ValueAggregator {
  IntegerSummary.Mode _mode;

  public IntegerTupleSketchAggregator(IntegerSummary.Mode mode) {
    _mode = mode;
  }

  @Override
  public Object aggregate(Object value1, Object value2) {
    Sketch<IntegerSummary> first = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize((byte[]) value1);
    Sketch<IntegerSummary> second = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize((byte[]) value2);
    Sketch<IntegerSummary> result = new Union<>(new IntegerSummarySetOperations(_mode, _mode)).union(first, second);
    return ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(result);
  }
}
