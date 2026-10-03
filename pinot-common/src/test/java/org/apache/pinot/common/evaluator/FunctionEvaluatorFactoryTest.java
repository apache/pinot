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
package org.apache.pinot.common.evaluator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class FunctionEvaluatorFactoryTest {
  private static final String GROOVY_EXPRESSION = "Groovy({value + 1}, value)";
  private static final String DISABLED_GROOVY_MESSAGE = String.format(
      "Groovy ingestion functions are disabled. Set '%s=false' to enable them",
      CommonConstants.Groovy.DISABLE_INGESTION_GROOVY);

  @BeforeMethod
  public void setUp() {
    FunctionEvaluatorFactory.setIngestionGroovyDisabled(true);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    FunctionEvaluatorFactory.setIngestionGroovyDisabled(true);
  }

  @Test
  public void testGroovyDisabledByDefault() {
    assertTrue(FunctionEvaluatorFactory.isIngestionGroovyDisabled());

    IllegalStateException validationError = expectThrows(IllegalStateException.class,
        () -> FunctionEvaluatorFactory.validateIngestionGroovyPolicy(GROOVY_EXPRESSION));
    assertEquals(validationError.getMessage(), DISABLED_GROOVY_MESSAGE);

    // Invalid Groovy must be rejected by policy before compilation is attempted.
    IllegalStateException constructionError = expectThrows(IllegalStateException.class,
        () -> FunctionEvaluatorFactory.getExpressionEvaluator("Groovy({not valid groovy!!!})"));
    assertEquals(constructionError.getMessage(), DISABLED_GROOVY_MESSAGE);

    IllegalStateException caseInsensitiveError = expectThrows(IllegalStateException.class,
        () -> FunctionEvaluatorFactory.getExpressionEvaluator("groovy({1})"));
    assertEquals(caseInsensitiveError.getMessage(), DISABLED_GROOVY_MESSAGE);
  }

  @Test
  public void testExplicitGroovyOptIn() {
    FunctionEvaluatorFactory.setIngestionGroovyDisabled(false);
    assertFalse(FunctionEvaluatorFactory.isIngestionGroovyDisabled());
    FunctionEvaluatorFactory.validateIngestionGroovyPolicy(GROOVY_EXPRESSION);

    FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator(GROOVY_EXPRESSION);
    assertTrue(evaluator instanceof GroovyFunctionEvaluator);
    assertEquals(evaluator.evaluate(new Object[]{1}), 2);
  }

  @Test
  public void testExplicitPoliciesAreContextScoped() {
    assertTrue(FunctionEvaluatorFactory.isIngestionGroovyDisabled());

    FunctionEvaluator enabledEvaluator =
        FunctionEvaluatorFactory.getExpressionEvaluator(GROOVY_EXPRESSION, false);
    assertEquals(enabledEvaluator.evaluate(new Object[]{1}), 2);
    IllegalStateException disabledError = expectThrows(IllegalStateException.class,
        () -> FunctionEvaluatorFactory.getExpressionEvaluator(GROOVY_EXPRESSION, true));
    assertEquals(disabledError.getMessage(), DISABLED_GROOVY_MESSAGE);
    assertTrue(FunctionEvaluatorFactory.isIngestionGroovyDisabled());
  }

  @Test
  public void testStandaloneSystemPropertyOptIn() {
    assertTrue(FunctionEvaluatorFactory.resolveIngestionGroovyDisabled(null));
    assertTrue(FunctionEvaluatorFactory.resolveIngestionGroovyDisabled("invalid"));
    assertTrue(FunctionEvaluatorFactory.resolveIngestionGroovyDisabled("true"));
    assertFalse(FunctionEvaluatorFactory.resolveIngestionGroovyDisabled(" false "));

    String configKey = CommonConstants.Groovy.DISABLE_INGESTION_GROOVY;
    String previousValue = System.getProperty(configKey);
    try {
      System.clearProperty(configKey);
      FunctionEvaluatorFactory.resetIngestionGroovyDisabledFromSystemProperty();
      assertTrue(FunctionEvaluatorFactory.isIngestionGroovyDisabled());

      System.setProperty(configKey, "false");
      FunctionEvaluatorFactory.resetIngestionGroovyDisabledFromSystemProperty();
      assertFalse(FunctionEvaluatorFactory.isIngestionGroovyDisabled());

      System.setProperty(configKey, "invalid");
      FunctionEvaluatorFactory.resetIngestionGroovyDisabledFromSystemProperty();
      assertTrue(FunctionEvaluatorFactory.isIngestionGroovyDisabled());
    } finally {
      if (previousValue == null) {
        System.clearProperty(configKey);
      } else {
        System.setProperty(configKey, previousValue);
      }
      FunctionEvaluatorFactory.resetIngestionGroovyDisabledFromSystemProperty();
    }
  }

  @Test
  public void testBuiltInTransformUnaffected() {
    FunctionEvaluatorFactory.validateIngestionGroovyPolicy("reverse(source)");
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("destination", FieldSpec.DataType.STRING, true);
    fieldSpec.setTransformFunction("reverse(source)");

    FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
    GenericRow row = new GenericRow();
    row.putValue("source", "Pinot");
    assertEquals(evaluator.evaluate(row), "toniP");
  }

  @Test
  public void testImplicitMapTransformsUnaffected() {
    DimensionFieldSpec keysFieldSpec =
        new DimensionFieldSpec("attributes__KEYS", FieldSpec.DataType.STRING, false);
    DimensionFieldSpec valuesFieldSpec =
        new DimensionFieldSpec("attributes__VALUES", FieldSpec.DataType.INT, false);
    FunctionEvaluator keysEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(keysFieldSpec);
    FunctionEvaluator valuesEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(valuesFieldSpec);
    assertFalse(keysEvaluator instanceof GroovyFunctionEvaluator);
    assertFalse(valuesEvaluator instanceof GroovyFunctionEvaluator);
    assertEquals(keysEvaluator.getArguments(), List.of("attributes"));
    assertEquals(valuesEvaluator.getArguments(), List.of("attributes"));

    Map<String, Integer> attributes = new LinkedHashMap<>();
    attributes.put("z", 2);
    attributes.put("a", 1);
    GenericRow row = new GenericRow();
    row.putValue("attributes", attributes);

    Object keys = keysEvaluator.evaluate(row);
    Object values = valuesEvaluator.evaluate(new Object[]{attributes});
    assertTrue(keys instanceof ArrayList);
    assertTrue(values instanceof ArrayList);
    assertEquals(keys, List.of("a", "z"));
    assertEquals(values, List.of(1, 2));
    assertNull(keysEvaluator.evaluate(new Object[]{null}));

    Map<String, Integer> reverseSortedAttributes = new TreeMap<>(java.util.Comparator.reverseOrder());
    reverseSortedAttributes.putAll(attributes);
    assertEquals(keysEvaluator.evaluate(new Object[]{reverseSortedAttributes}), List.of("a", "z"));
    assertEquals(valuesEvaluator.evaluate(new Object[]{reverseSortedAttributes}), List.of(1, 2));
  }

  @Test
  public void testDirectGroovyEvaluatorIsNotGovernedByIngestionPolicy() {
    GroovyFunctionEvaluator evaluator = new GroovyFunctionEvaluator("Groovy({1})");
    assertEquals(evaluator.evaluate(new Object[]{}), 1);
  }
}
