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
package org.apache.pinot.segment.local.aggregator;

import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class IntegerTupleSketchValueAggregatorTest {

  private byte[] sketchContaining(String key, int value) {
    IntegerSketch is = new IntegerSketch(16, IntegerSummary.Mode.Sum);
    is.update(key, value);
    return is.compact().toByteArray();
  };

  @Test
  public void initialShouldParseASketch() {
    IntegerTupleSketchValueAggregator agg = new IntegerTupleSketchValueAggregator(IntegerSummary.Mode.Sum);
    assertEquals(agg.getInitialAggregatedValue(sketchContaining("hello world", 1)).getEstimate(), 1.0);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    IntegerSketch s1 = new IntegerSketch(16, IntegerSummary.Mode.Sum);
    IntegerSketch s2 = new IntegerSketch(16, IntegerSummary.Mode.Sum);
    s1.update("a", 1);
    s2.update("b", 1);
    IntegerTupleSketchValueAggregator agg = new IntegerTupleSketchValueAggregator(IntegerSummary.Mode.Sum);
    Sketch<IntegerSummary> merged = agg.applyAggregatedValue(s1, s2);
    assertEquals(merged.getEstimate(), 2.0);

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), agg.serializeAggregatedValue(merged).length);
  }

  @Test
  public void applyRawValueShouldUnion() {
    IntegerSketch s1 = new IntegerSketch(16, IntegerSummary.Mode.Sum);
    IntegerSketch s2 = new IntegerSketch(16, IntegerSummary.Mode.Sum);
    s1.update("a", 1);
    s2.update("b", 1);
    IntegerTupleSketchValueAggregator agg = new IntegerTupleSketchValueAggregator(IntegerSummary.Mode.Sum);
    Sketch<IntegerSummary> merged = agg.applyRawValue(s1, agg.serializeAggregatedValue(s2));
    assertEquals(merged.getEstimate(), 2.0);

    // and should update the max size
    assertEquals(agg.getMaxAggregatedValueByteSize(), agg.serializeAggregatedValue(merged).length);
  }
}
