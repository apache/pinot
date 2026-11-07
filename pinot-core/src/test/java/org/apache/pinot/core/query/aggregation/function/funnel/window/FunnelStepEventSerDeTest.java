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
package org.apache.pinot.core.query.aggregation.function.funnel.window;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction.SerializedResult;
import org.apache.pinot.core.query.aggregation.function.funnel.FunnelStepEvent;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class FunnelStepEventSerDeTest {
  private static final Random RANDOM = new Random();
  private static final FunnelMaxStepAggregationFunction FUNCTION = new FunnelMaxStepAggregationFunction(
      List.of(ExpressionContext.forIdentifier("ts"), ExpressionContext.forLiteral(Literal.longValue(1000L)),
          ExpressionContext.forLiteral(Literal.intValue(2)), ExpressionContext.forIdentifier("step0"),
          ExpressionContext.forIdentifier("step1")), false);

  @Test
  public void testStepEventsRoundTrip() {
    for (int i = 0; i < 100; i++) {
      int numEvents = RANDOM.nextInt(1000);
      PriorityQueue<FunnelStepEvent> expected = new PriorityQueue<>();
      for (int j = 0; j < numEvents; j++) {
        expected.add(new FunnelStepEvent(RANDOM.nextLong(), RANDOM.nextInt()));
      }
      PriorityQueue<FunnelStepEvent> actual = roundTrip(expected);
      assertEquals(actual.size(), expected.size());
      while (!actual.isEmpty()) {
        assertEquals(actual.poll(), expected.poll());
      }
    }
  }

  @Test
  public void testEmptyStepEvents() {
    assertTrue(roundTrip(new PriorityQueue<>()).isEmpty());
  }

  private static PriorityQueue<FunnelStepEvent> roundTrip(PriorityQueue<FunnelStepEvent> stepEvents) {
    SerializedResult serialized = FUNCTION.serializeIntermediateResult(stepEvents);
    return FUNCTION.deserializeIntermediateResult(
        new CustomObject(serialized.getType(), ByteBuffer.wrap(serialized.getBytes())));
  }
}
