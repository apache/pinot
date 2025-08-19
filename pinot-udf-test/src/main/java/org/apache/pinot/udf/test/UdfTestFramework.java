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
package org.apache.pinot.udf.test;

import com.google.common.collect.Maps;
import com.google.common.math.DoubleMath;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;

/// This is the entry paint for the UDF test framework. It allows to run tests for UDFs using a given cluster.
public class UdfTestFramework {

  private static final EquivalenceLevel[] EQUIVALENCE_LEVELS = new EquivalenceLevel[]{
      EquivalenceLevel.EQUAL,
      EquivalenceLevel.BIG_DECIMAL_AS_DOUBLE,
      EquivalenceLevel.NUMBER_AS_DOUBLE
  };
  private final Set<Udf> _udfs;
  private final UdfTestCluster _cluster;
  private final Set<UdfTestScenario> _scenarios;
  private final ExecutorService _executorService;

  public UdfTestFramework(Set<Udf> udfs, UdfTestCluster cluster,
      Set<UdfTestScenario> scenarios, ExecutorService executorService) {
    _udfs = udfs;
    _cluster = cluster;
    _scenarios = scenarios;
    _executorService = executorService;
  }

  public static UdfTestFramework fromServiceLoader(UdfTestCluster cluster,
      ExecutorService executorService) {
    Set<Udf> udfs = ServiceLoader.load(Udf.class).stream()
        .map(ServiceLoader.Provider::get)
        .collect(Collectors.toSet());
    Set<UdfTestScenario> scenarios = ServiceLoader.load(UdfTestScenario.Factory.class).stream()
        .map(ServiceLoader.Provider::get)
        .map(factory -> factory.create(cluster))
        .collect(Collectors.toSet());

    return new UdfTestFramework(udfs, cluster, scenarios, executorService);
  }

  public Set<Udf> getUdfs() {
    return _udfs;
  }

  public Set<UdfTestScenario> getScenarios() {
    return _scenarios;
  }

  public void startUp() {
    PinotFunctionEnvGenerator.prepareEnvironment(_cluster, _udfs);
  }

  /// Executes all UDFs in all scenarios and returns the results.
  public UdfTestResult execute()
      throws InterruptedException {
    Map<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>>> asyncResults
        = Maps.newHashMapWithExpectedSize(_udfs.size());
    for (Udf udf : _udfs) {
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> scenarioResults
          = Maps.newHashMapWithExpectedSize(_scenarios.size());
      executeAsync(udf, scenarioResults);
      asyncResults.put(udf, scenarioResults);
    }

    return byUdf(asyncResults);
  }

  /// Executes a single UDF in all scenarios and returns the results.
  public UdfTestResult.ByScenario execute(Udf udf)
      throws InterruptedException {
    Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> scenarioResults
        = Maps.newHashMapWithExpectedSize(_scenarios.size());
    executeAsync(udf, scenarioResults);

    return byScenario(scenarioResults);
  }

  private void executeAsync(
      Udf udf,
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> scenarioResults
  ) {
    for (UdfTestScenario scenario : _scenarios) {
      Set<UdfSignature> udfSignatures = udf.getExamples().keySet();
      Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>> signatureResults
          = Maps.newHashMapWithExpectedSize(udfSignatures.size());
      for (UdfSignature signature: udfSignatures) {
        signatureResults.put(signature, _executorService.submit(() -> scenario.execute(udf, signature)));
      }
      scenarioResults.put(scenario, signatureResults);
    }
  }

  private UdfTestResult byUdf(
      Map<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>>> tasks
  ) throws InterruptedException {
    Map<Udf, UdfTestResult.ByScenario> results = Maps.newHashMapWithExpectedSize(tasks.size());
    for (Map.Entry<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>>> entry
        : tasks.entrySet()) {
      Udf udf = entry.getKey();
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> scenarioTasks
          = entry.getValue();
      results.put(udf, byScenario(scenarioTasks));
    }
    return new UdfTestResult(results);
  }

  private UdfTestResult.ByScenario byScenario(
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> scenarioTasks
  ) throws InterruptedException {
    Map<UdfTestScenario, UdfTestResult.BySignature> results = Maps.newHashMapWithExpectedSize(scenarioTasks.size());
    for (Map.Entry<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>>> entry
        : scenarioTasks.entrySet()) {
      UdfTestScenario scenario = entry.getKey();
      Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>> tasks = entry.getValue();
      results.put(scenario, bySignature(tasks));
    }
    return new UdfTestResult.ByScenario(results);
  }

  private UdfTestResult.BySignature bySignature(
      Map<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>> tasks
  ) throws InterruptedException {
    Map<UdfSignature, ResultByExample> results = Maps.newHashMapWithExpectedSize(tasks.size());
    for (Map.Entry<UdfSignature, Future<Map<UdfExample, UdfExampleResult>>> entry : tasks.entrySet()) {
      UdfSignature signature = entry.getKey();
      Future<Map<UdfExample, UdfExampleResult>> task = entry.getValue();
      ResultByExample result = resolve(task);
      results.put(signature, result);
    }
    return new UdfTestResult.BySignature(results);
  }

  private ResultByExample resolve(Future<Map<UdfExample, UdfExampleResult>> task)
      throws InterruptedException {
    try {
      Map<UdfExample, UdfExampleResult> result = task.get();
      Map<UdfExample, EquivalenceLevel> comparisons = Maps.newHashMapWithExpectedSize(result.size());
      Map<UdfExample, String> errors = Maps.newHashMapWithExpectedSize(result.size());
      for (Map.Entry<UdfExample, UdfExampleResult> entry : result.entrySet()) {
        try {
          EquivalenceLevel equivalence = compareResult(entry.getValue());
          comparisons.put(entry.getKey(), equivalence);
        } catch (Exception e) {
          errors.put(entry.getKey(), e.getMessage());
        }
      }
      return new ResultByExample.Partial(result, comparisons, errors);
    } catch (ExecutionException e) {
      if (e.getCause().getMessage().contains("Unsupported function")) {
        return new ResultByExample.Failure("Unsupported");
      }
      if (e.getCause().getMessage().contains("Caught exception while doing operator")) {
        return new ResultByExample.Failure("Operator execution error");
      }
      return new ResultByExample.Failure(e.getCause().getMessage());
    }
  }

  private EquivalenceLevel compareResult(UdfExampleResult result) {
    Object actualResult = result.getActualResult();
    Object expectedResult = result.getExpectedResult();

    for (EquivalenceLevel equivalence : EQUIVALENCE_LEVELS) {
      try {
        equivalence.check(expectedResult, actualResult);
        return equivalence;
      } catch (AssertionError e) {
        // Continue to the next equivalence if the current one fails
      }
    }
    throw new RuntimeException("Unexpected value");
  }

  public enum EquivalenceLevel {
    EQUAL,
    BIG_DECIMAL_AS_DOUBLE,
    NUMBER_AS_DOUBLE,
    ERROR;

    public void check(@Nullable Object expected, @Nullable Object actual) throws AssertionError {
      expected = canonizeObject(expected);
      actual = canonizeObject(actual);
      switch (this) {
        case EQUAL:
          if (expected instanceof Double && actual instanceof Double
            && !doubleEquals((Double) expected, (Double) actual)) {
            throw new AssertionError(describeDiscrepancy(expected, actual));
          }
          if (!Objects.equals(expected, actual)) {
            throw new AssertionError(describeDiscrepancy(expected, actual));
          }
          break;
        case NUMBER_AS_DOUBLE:
          if (expected == null && actual == null) {
            return; // Both are null, considered equal
          }
          if (expected instanceof BigDecimal && actual instanceof String) {
            // Big decimals are sent as strings through the wire protocol, so we need to convert them
            try {
              actual = new BigDecimal((String) actual);
            } catch (NumberFormatException e) {
              throw new AssertionError("Expected a number as actual value but got the String: " + actual);
            }
          }
          if (!(expected instanceof Number) || !(actual instanceof Number)) {
            Class<?> expectedClass = expected != null ? expected.getClass() : null;
            Class<?> actualClass = actual != null ? actual.getClass() : null;
            throw new AssertionError("Both expected and actual should be numbers for NUMBER_AS_DOUBLE "
                + "comparison, but got: expected=" + expected + "(of type " + expectedClass + ")"
                + ", actual=" + actual + "(of type " + actualClass + ")");
          }
          if (!doubleEquals(((Number) expected).doubleValue(), ((Number) actual).doubleValue())) {
            throw new AssertionError(describeDiscrepancy(expected, actual));
          }
          break;
        case BIG_DECIMAL_AS_DOUBLE: {
          if (expected instanceof BigDecimal) {
            NUMBER_AS_DOUBLE.check(expected, actual);
          } else {
            EQUAL.check(expected, actual);
          }
          break;
        }
        case ERROR:
          throw new UnsupportedOperationException("Comparison type ERROR is not supported");
        default:
          throw new IllegalArgumentException("Unknown comparison type: " + this);
      }
    }

    private static String describeDiscrepancy(@Nullable Object expected, @Nullable Object actual) {
      String expectedDesc = expected == null ? "null" : expected + " (" + expected.getClass().getSimpleName() + ")";
      String actualDesc = actual == null ? "null" : actual + " (" + actual.getClass().getSimpleName() + ")";
      return "Expected: " + expectedDesc + ", but got: " + actualDesc;
    }

    private static Object canonizeObject(@Nullable Object value) {
      if (value == null) {
        return null;
      }
      if (value.getClass().isArray()) {
        Class<?> componentType = value.getClass().getComponentType();
        if (componentType == int.class) {
          return Arrays.stream((int[]) value).boxed().collect(Collectors.toList());
        } else if (componentType == long.class) {
          return Arrays.stream((long[]) value).boxed().collect(Collectors.toList());
        } else if (componentType == double.class) {
          return Arrays.stream((double[]) value).boxed().collect(Collectors.toList());
        } else if (componentType == float.class) {
          // Convert float array to List<Float> for consistency
          float[] floatArray = (float[]) value;
          ArrayList<Float> list = new ArrayList<>(floatArray.length);
          for (float f : floatArray) {
            list.add(f);
          }
          return list;
        } else if (componentType == byte.class) {
          // Convert byte array to List<Byte> for consistency
          byte[] byteArray = (byte[]) value;
          ArrayList<Byte> list = new ArrayList<>(byteArray.length);
          for (byte b : byteArray) {
            list.add(b);
          }
          return list;
        } else {
          return Arrays.asList((Object[]) value);
        }
      }
      return value;
    }
  }

  private static boolean doubleEquals(double d1, double d2) {
    double epsilon = 0.0001d;
    return DoubleMath.fuzzyEquals(d1, d2, epsilon);
  }
}
