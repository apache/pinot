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
package org.apache.pinot.segment.local.function;

import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that the deprecated {@code org.apache.pinot.segment.local.function} shim layer
 * is wired through to the canonical implementations in {@code org.apache.pinot.common.evaluator}.
 */
@SuppressWarnings("deprecation")
public class FunctionPackageCompatibilityTest {

  @Test
  public void testLegacyFactoryReturnsCanonicalInbuiltEvaluator() {
    FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator("reverse(testColumn)");
    assertNotNull(evaluator);
    assertTrue(evaluator instanceof org.apache.pinot.common.evaluator.InbuiltFunctionEvaluator);

    GenericRow row = new GenericRow();
    row.putValue("testColumn", "value");
    assertEquals(evaluator.evaluate(row), "eulav");
  }

  @Test
  public void testLegacyGroovyApiStillWorks()
      throws Exception {
    GroovyStaticAnalyzerConfig config = GroovyStaticAnalyzerConfig.createDefault();
    GroovyFunctionEvaluator.setGroovyStaticAnalyzerConfig(config);
    try {
      GroovyFunctionEvaluator.parseGroovyScript("Groovy({a + 1}, a)");
      FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator("Groovy({a + 1}, a)");
      assertTrue(evaluator instanceof org.apache.pinot.common.evaluator.GroovyFunctionEvaluator);
      assertEquals(evaluator.evaluate(new Object[]{1}), 2);
    } finally {
      GroovyFunctionEvaluator.setGroovyStaticAnalyzerConfig(null);
    }
  }
}
