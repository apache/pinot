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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.udf.test.PinotFunctionEnvGenerator;
import org.apache.pinot.udf.test.UdfExampleResult;
import org.apache.pinot.udf.test.UdfTestCluster;
import org.apache.pinot.udf.test.UdfTestScenario;


/// An abstract class for UDF test scenarios, providing common functionality like running SQL queries on a given
/// PinotFunctionTestCluster and extracting results from the query output.
public abstract class AbstractUdfTestScenario implements UdfTestScenario {

  protected final UdfTestCluster _cluster;
  private final boolean _nullHandlingEnabled;

  public AbstractUdfTestScenario(UdfTestCluster cluster, boolean nullHandlingEnabled) {
    _cluster = cluster;
    _nullHandlingEnabled = nullHandlingEnabled;
  }

  protected boolean isNullHandlingEnabled() {
    return _nullHandlingEnabled;
  }

  protected String replaceCommonVariables(
      Udf udf,
      UdfSignature signature,
      boolean nullHandling,
      /* language=sql*/ String templateSql) {
    String call = udf.asSqlCall(udf.getMainFunctionName(), PinotFunctionEnvGenerator.getArgsForCall(signature));
    return templateSql
        .replaceAll("@call", call)
        .replaceAll("@table", PinotFunctionEnvGenerator.getTableName(udf))
        .replaceAll("@udfCol", PinotFunctionEnvGenerator.getUdfColumnName())
        .replaceAll("@udfName", udf.getMainFunctionName())
        .replaceAll("@testCol", PinotFunctionEnvGenerator.getTestColumnName())
        .replaceAll("@resultCol", PinotFunctionEnvGenerator.getResultColumnName(signature, nullHandling))
        // Important, we need to replace the @testCol and @result after replacing @test and @resultCol respectively
        .replaceAll("@test", "test")
        .replaceAll("@result", "result")
        .replaceAll("@signatureCol", PinotFunctionEnvGenerator.getSignatureColumnName())
        .replaceAll("@signature", signature.toString());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  protected Map<UdfExample, UdfExampleResult> extractResultsByCase(
      Udf udf,
      UdfSignature signature,
      Iterator<GenericRow> rows) {
    Map<UdfExample, UdfExampleResult> result = new HashMap<>();

    Map<String, UdfExample> testById = udf.getExamples()
        .get(signature)
        .stream()
        .collect(Collectors.toMap(UdfExample::getId, testCase -> testCase));

    while (rows.hasNext()) {
      GenericRow row = rows.next();
      String testId = (String) row.getValue("test");
      UdfExample test = testById.get(testId);
      assert test != null : "Test case not found for id: " + testId;

      Object expectedResult = test.getResult(isNullHandlingEnabled());
      result.put(test, UdfExampleResult.success(test, row.getValue("result"), expectedResult));
    }
    return result;
  }
}
