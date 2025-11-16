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
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.udf.test.PinotFunctionEnvGenerator;
import org.apache.pinot.udf.test.UdfExampleResult;
import org.apache.pinot.udf.test.UdfTestCluster;
import org.apache.pinot.udf.test.UdfTestScenario;


/// A test scenario where the UDF is executed on an intermediate stage of the MSE.
public class IntermediateUdfTestScenario extends AbstractUdfTestScenario {

  public IntermediateUdfTestScenario(UdfTestCluster cluster, UdfExample.NullHandling nullHandlingMode) {
    super(cluster, nullHandlingMode);
  }

  @Override
  public String getTitle() {
    return "MSE intermediate stage (" + (getNullHandlingMode() == UdfExample.NullHandling.ENABLED ? "with" : "without")
        + " null handling)";
  }

  @Override
  public String getDescription() {
    return "This scenario tests the UDF as an intermediate stage of the MSE. ";
  }

  @Override
  public Map<UdfExample, UdfExampleResult> execute(
      Udf udf,
      UdfSignature signature) {
    List<UdfParameter> params = signature.getParameters();

    Set<UdfExample> examples = udf.getExamples().get(signature);
    Map<UdfExample, UdfExampleResult> results = Maps.newHashMapWithExpectedSize(examples.size());
    // TODO: Look for a way to force the UDF to be executed on an intermediate stage of the MSE when it has 0 params.
    if (params.isEmpty()) {
      for (UdfExample example : examples) {
        String errorMessage = "Need at least one parameter to execute the UDF in an intermediate stage";
        results.put(example, UdfExampleResult.error(example, errorMessage));
      }
      return results;
    }

    String firstCol = PinotFunctionEnvGenerator.getParameterColumnName(signature, 0);
    StringBuilder otherCols = new StringBuilder();
    for (int i = 1; i < params.size(); i++) {
      String colName = PinotFunctionEnvGenerator.getParameterColumnName(signature, i);
      otherCols.append(",\n    t1.")
          .append(colName)
          .append(" AS ")
          .append(colName);
    }

    // language=sql
    String sqlTemplate = (""
            + "WITH fakeTable AS (\n" // this table is used to make sure the call is made on an intermediate stage
            + "  SELECT \n"
            + "    t1.@udfCol, \n"
            + "    t1.@signatureCol, \n"
            + "    t1.@testCol, \n"
            // Calcite is not smart enough to know that this coalesce could be simplified to just t1.@firstCol,
            // and given it uses cols from two different columns it has to be executed after the join.
            + "    coalesce(t1.@firstCol, t2.@firstCol) as @firstCol" //
            + "@otherCols\n"
            + "  FROM @table_OFFLINE AS t1 \n"
            + "  JOIN @table_OFFLINE AS t2 \n"
            + "  ON t1.@testCol = t2.@testCol AND t1.@signatureCol = t2.@signatureCol and t1.udf = t2.udf \n"
            + ")\n"
            + "SELECT \n"
            + "  @testCol as test, \n"
            // This call will use the result of the coalesce as argument. This means it must be executed after the join,
            // which means that it has to be executed in an intermediate stage.
            + "  @call AS result\n"
            + "FROM fakeTable \n"
            + "WHERE @signatureCol = '@signature' \n"
            + "  AND @udfCol = '@udfName' \n"
            + "  AND @testCol = '@example' \n")
        .replace("@firstCol", firstCol)
        .replace("@otherCols", otherCols.toString());

    UdfTestCluster.ExecutionContext context = new UdfTestCluster.ExecutionContext(
        getNullHandlingMode(),
        true);

    return extractResultsByCase(udf, signature, context, sqlTemplate);
  }

  /// A factory that creates an instance of this scenario with null handling enabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithNullHandlingFactory() {
      super(cluster -> new IntermediateUdfTestScenario(cluster, UdfExample.NullHandling.ENABLED));
    }
  }

  /// A factory that creates an instance of this scenario with null handling disabled
  @AutoService(UdfTestScenario.Factory.class)
  public static class WithoutNullHandlingFactory extends UdfTestScenario.Factory.FromCluster {
    public WithoutNullHandlingFactory() {
      super(cluster -> new IntermediateUdfTestScenario(cluster, UdfExample.NullHandling.DISABLED));
    }
  }
}
