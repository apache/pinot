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
package org.apache.pinot.udf.test.scenarios;

import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.udf.test.UdfExampleResult;
import org.apache.pinot.udf.test.UdfTestCluster;
import org.apache.pinot.udf.test.UdfTestScenario;


/// A test scenario where the UDF is executed as a predicate, which is always evaluated as a ScalarFunction.
public class PredicateUdfTestScenario extends AbstractUdfTestScenario {
  public PredicateUdfTestScenario(UdfTestCluster cluster, UdfExample.NullHandling nullHandlingMode) {
    super(cluster, nullHandlingMode);
  }

  @Override
  public String getTitle() {
    return "SSE predicate (" + (getNullHandlingMode() == UdfExample.NullHandling.ENABLED ? "with" : "without")
        + " null handling)";
  }

  @Override
  public String getDescription() {
    return "This scenario tests the UDF as a predicate in the SSE.";
  }

  @Override
  public Map<UdfExample, UdfExampleResult> execute(Udf udf, UdfSignature signature) {

    // language=sql
    String sqlTemplate = ""
        + "SELECT \n"
        + "  @testCol as test,"
        + "  true as result\n"
        + "FROM @table \n"
        + "WHERE @signatureCol = '@signature' \n"
        + "  AND @udfCol = '@udfName' \n"
        + "  AND ( \n"
        + "    @call = @resultCol OR \n"
        + "    @call IS NULL AND @resultCol IS NULL \n"
        + "  )\n"
        + "  AND @testCol = '@example' \n";
    UdfTestCluster.ExecutionContext context = new UdfTestCluster.ExecutionContext(
        getNullHandlingMode(),
        false);
    Map<UdfExample, UdfExampleResult> queryResult = extractResultsByCase(udf, signature, context, sqlTemplate);

    Set<UdfExample> successfulCases = queryResult.values()
        .stream()
        .map(UdfExampleResult::getTest)
        .collect(Collectors.toSet());

    Set<UdfExample> examples = udf.getExamples().get(signature);
    Map<UdfExample, UdfExampleResult> result = new HashMap<>();
    Sets.SetView<UdfExample> expectedPositives = Sets.intersection(examples, successfulCases);
    try {
      for (UdfExample expectedPositive : expectedPositives) {
        result.put(expectedPositive, UdfExampleResult.success(expectedPositive, true, true));
      }
    } catch (RuntimeException e) {
      throw e;
    }

    if (expectedPositives.size() != udf.getExamples().size()) {
      Sets.SetView<UdfExample> unexpectedNegatives = Sets.difference(examples, successfulCases);
      for (UdfExample unexpected : unexpectedNegatives) {
        result.put(unexpected, UdfExampleResult.success(unexpected, false, true));
      }
      Sets.SetView<UdfExample> unexpectedPositives = Sets.difference(successfulCases, examples);
      for (UdfExample unexpected : unexpectedPositives) {
        result.put(unexpected, UdfExampleResult.success(unexpected, true, false));
      }
    }
    return result;
  }

  /// A factory that creates an instance of this scenario with null handling enabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithNullHandlingFactory() {
      super(cluster -> new PredicateUdfTestScenario(cluster, UdfExample.NullHandling.ENABLED));
    }
  }

  /// A factory that creates an instance of this scenario with null handling disabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithoutNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithoutNullHandlingFactory() {
      super(cluster -> new PredicateUdfTestScenario(cluster, UdfExample.NullHandling.DISABLED));
    }
  }
}
