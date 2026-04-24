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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.pinot.common.utils.ThrottledLogger;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Parity tests for {@link IngestionFunctionEvaluation} (shared by {@link ExpressionTransformer} and
 * {@link org.apache.pinot.segment.local.recordtransformer.enricher.function.CustomFunctionEnricher}).
 */
public class IngestionFunctionEvaluationTest {

  @Test
  public void testApplyEnricherEvaluationsAlwaysOverwrites() {
    org.apache.pinot.segment.local.function.FunctionEvaluator evaluator =
        mock(org.apache.pinot.segment.local.function.FunctionEvaluator.class);
    when(evaluator.evaluate(any())).thenReturn("enriched");
    LinkedHashMap<String, org.apache.pinot.segment.local.function.FunctionEvaluator> map = new LinkedHashMap<>();
    map.put("out", evaluator);
    GenericRow record = new GenericRow();
    record.putValue("out", "stale");
    IngestionFunctionEvaluation.applyEnricherEvaluations(record, map);
    assertEquals(record.getValue("out"), "enriched");
    verify(evaluator).evaluate(record);
  }

  @Test
  public void testApplyExpressionTransformationsFillsWhenColumnNull() {
    FunctionEvaluator evaluator = mock(FunctionEvaluator.class);
    when(evaluator.evaluate(any())).thenReturn(99L);
    LinkedHashMap<String, FunctionEvaluator> map = new LinkedHashMap<>();
    map.put("c", evaluator);
    GenericRow record = new GenericRow();
    assertNull(record.getValue("c"));
    ThrottledLogger throttledLogger = mock(ThrottledLogger.class);
    IngestionFunctionEvaluation.applyExpressionTransformations(record, map, false, false,
        Collections.emptySet(), throttledLogger);
    assertEquals(record.getValue("c"), 99L);
  }

  @Test
  public void testApplyExpressionTransformationsSkipsWhenPrimitivePresent() {
    FunctionEvaluator evaluator = mock(FunctionEvaluator.class);
    LinkedHashMap<String, FunctionEvaluator> map = new LinkedHashMap<>();
    map.put("c", evaluator);
    GenericRow record = new GenericRow();
    record.putValue("c", 1);
    ThrottledLogger throttledLogger = mock(ThrottledLogger.class);
    IngestionFunctionEvaluation.applyExpressionTransformations(record, map, false, false,
        Collections.emptySet(), throttledLogger);
    assertEquals(record.getValue("c"), 1);
    verifyNoInteractions(evaluator);
  }

  @Test
  public void testApplyExpressionTransformationsContinueOnErrorMarksIncomplete() {
    FunctionEvaluator evaluator = mock(FunctionEvaluator.class);
    when(evaluator.evaluate(any())).thenThrow(new RuntimeException("eval failed"));
    LinkedHashMap<String, FunctionEvaluator> map = new LinkedHashMap<>();
    map.put("c", evaluator);
    GenericRow record = new GenericRow();
    ThrottledLogger throttledLogger = mock(ThrottledLogger.class);
    IngestionFunctionEvaluation.applyExpressionTransformations(record, map, true, false,
        Collections.emptySet(), throttledLogger);
    assertTrue(record.isIncomplete());
  }
}
