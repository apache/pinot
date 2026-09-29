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

import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExample.NullHandling;
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
  private final NullHandling _nullHandlingMode;

  public AbstractUdfTestScenario(UdfTestCluster cluster, NullHandling nullHandlingMode) {
    _cluster = cluster;
    _nullHandlingMode = nullHandlingMode;
  }

  protected NullHandling getNullHandlingMode() {
    return _nullHandlingMode;
  }

  protected String replaceCommonVariables(
      Udf udf,
      UdfSignature signature,
      NullHandling nullHandling,
      /* language=sql*/ String templateSql) {
    return templateSql
        .replace("@table", PinotFunctionEnvGenerator.getTableName(udf))
        .replace("@udfCol", PinotFunctionEnvGenerator.getUdfColumnName())
        .replace("@udfName", udf.getMainCanonicalName())
        .replace("@testCol", PinotFunctionEnvGenerator.getTestColumnName())
        .replace("@resultCol", PinotFunctionEnvGenerator.getResultColumnName(signature, nullHandling))
        // Important, we need to replace the @testCol and @resultCol before replacing @test and @result respectively
        .replace("@test", "test")
        .replace("@result", "result")
        .replace("@signatureCol", PinotFunctionEnvGenerator.getSignatureColumnName())
        .replace("@signature", signature.toString());
  }

  protected String replaceCall(
      Udf udf,
      UdfSignature signature,
      UdfExample example,
      /* language=sql*/ String templateSql) {
    List<String> argsForCall = PinotFunctionEnvGenerator.getArgsForCall(signature, example);
    String call = udf.asSqlCall(udf.getMainCanonicalName(), argsForCall);
    return templateSql
        .replace("@example", example.getId())
        .replace("@call", call);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  protected Map<UdfExample, UdfExampleResult> extractResultsByCase(
      Udf udf,
      UdfSignature signature,
      UdfTestCluster.ExecutionContext context,
      /* language=sql*/ String sqlTemplate) {
    Set<UdfExample> examples = udf.getExamples().get(signature);
    Map<UdfExample, UdfExampleResult> results = Maps.newHashMapWithExpectedSize(examples.size());

    String sqlTemplate2 = replaceCommonVariables(udf, signature, getNullHandlingMode(), sqlTemplate);

    for (UdfExample example : examples) {
      String sql = replaceCall(udf, signature, example, sqlTemplate2);
      Iterator<GenericRow> rows = _cluster.query(context, sql);

      if (!rows.hasNext()) {
        String errorMessage = "No results found for example: " + example.getId();
        results.put(example, UdfExampleResult.error(example, errorMessage));
        continue;
      }
      GenericRow row = rows.next();
      String testId = (String) row.getValue("test");
      if (testId != null && !testId.equals(example.getId())) {
        String errorMessage = "Test ID mismatch: expected " + example.getId() + ", found " + testId;
        results.put(example, UdfExampleResult.error(example, errorMessage));
        continue;
      }
      Object expectedResult = example.getResult(getNullHandlingMode());
      results.put(example, UdfExampleResult.success(example, row.getValue("result"), expectedResult));

      if (rows.hasNext()) {
        String errorMessage = "Multiple results found for example: " + example.getId();
        results.put(example, UdfExampleResult.error(example, errorMessage));
      }
    }

    return results;
  }
}
