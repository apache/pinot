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
package org.apache.pinot.compat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.representer.Representer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests expected query-error matching used by mixed-version compatibility suites.
public class QueryOpTest {

  @Test
  public void testMatchesExpectedErrorInExceptionArray()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        "{\"exceptions\":["
            + "{\"errorCode\":200,\"message\":\"java.lang.IllegalStateException: "
            + "Unsupported proto ColumnDataType: UNRECOGNIZED\"},"
            + "{\"errorCode\":200,\"message\":\"Unsupported proto ColumnDataType: UNRECOGNIZED\"}]}");

    assertTrue(QueryOp.hasExpectedError(response, "Unsupported proto ColumnDataType: UNRECOGNIZED"));
    assertFalse(QueryOp.hasExpectedError(response, "Unsupported proto ColumnDataType: VARIANT"));

    JsonNode mixedResponse = JsonUtils.stringToJsonNode(
        "{\"exceptions\":["
            + "{\"errorCode\":200,\"message\":\"Unsupported proto ColumnDataType: UNRECOGNIZED\"},"
            + "{\"errorCode\":200,\"message\":\"Server request timed out\"}]}");
    assertFalse(QueryOp.hasExpectedError(mixedResponse, "Unsupported proto ColumnDataType: UNRECOGNIZED"));

    JsonNode partialRowsResponse = JsonUtils.stringToJsonNode(
        "{\"resultTable\":{\"rows\":[[10]]},\"numRowsResultSet\":1,"
            + "\"exceptions\":[{\"message\":\"Unsupported proto ColumnDataType: UNRECOGNIZED\"}]}");
    assertFalse(QueryOp.hasExpectedError(partialRowsResponse, "Unsupported proto ColumnDataType: UNRECOGNIZED"));
  }

  @Test
  public void testMatchesExpectedErrorInExceptionObject()
      throws Exception {
    JsonNode response =
        JsonUtils.stringToJsonNode("{\"exceptions\":{\"message\":\"Unsupported proto ColumnDataType: UNRECOGNIZED\"}}");

    assertTrue(QueryOp.hasExpectedError(response, "ColumnDataType: UNRECOGNIZED"));
  }

  @Test
  public void testDoesNotMatchMissingOrMalformedExceptions()
      throws Exception {
    String expectedMessage = "Unsupported proto ColumnDataType: UNRECOGNIZED";
    assertFalse(QueryOp.hasExpectedError(null, expectedMessage));
    assertFalse(QueryOp.hasExpectedError(JsonUtils.stringToJsonNode("{}"), expectedMessage));
    assertFalse(QueryOp.hasExpectedError(JsonUtils.stringToJsonNode("{\"exceptions\":[]}"),
        expectedMessage));
    assertFalse(QueryOp.hasExpectedError(
        JsonUtils.stringToJsonNode("{\"exceptions\":[{\"errorCode\":200}]}"), expectedMessage));
    assertFalse(QueryOp.hasExpectedError(
        JsonUtils.stringToJsonNode("{\"exceptions\":[{\"message\":42}]}"), expectedMessage));
    assertFalse(QueryOp.hasExpectedError(
        JsonUtils.stringToJsonNode("{\"exceptions\":[{\"message\":\"" + expectedMessage + "\"}]}"), ""));
  }

  @Test
  public void testExpectedOutcomeConfigurationRequiresExactlyOneMode() {
    QueryOp queryOp = new QueryOp();
    assertFalse(queryOp.hasValidExpectedOutcomeConfiguration());

    queryOp.setExpectedResultsFileName("query-results/results.json");
    assertTrue(queryOp.hasValidExpectedOutcomeConfiguration());

    queryOp.setExpectedErrorMessageContains(" ");
    assertFalse(queryOp.hasValidExpectedOutcomeConfiguration());

    queryOp.setExpectedErrorMessageContains("Unsupported proto ColumnDataType: UNRECOGNIZED");
    assertFalse(queryOp.hasValidExpectedOutcomeConfiguration());

    queryOp.setExpectedResultsFileName(null);
    assertTrue(queryOp.hasValidExpectedOutcomeConfiguration());

    queryOp.setExpectedErrorMessageContains(" ");
    assertFalse(queryOp.hasValidExpectedOutcomeConfiguration());
  }

  @Test
  public void testRetriesAreExplicitAndBounded() {
    int[] attempts = {0};
    QueryOp queryOp = new QueryOp() {
      @Override
      boolean verifyQueries(int generationNumber) {
        return ++attempts[0] == 3;
      }
    };
    queryOp.setQueryFileName("queries/routing-ready.queries");
    queryOp.setExpectedResultsFileName("query-results/variant-wire.results");
    queryOp.setMaxAttempts(3);
    queryOp.setRetryDelayMs(0);

    assertTrue(queryOp.runOp(1));
    assertEquals(attempts[0], 3);

    queryOp.setMaxAttempts(0);
    assertFalse(queryOp.runOp(1));
    queryOp.setMaxAttempts(1);
    queryOp.setRetryDelayMs(-1);
    assertFalse(queryOp.runOp(1));
  }

  @Test
  public void testExpectedResultReaderReportsTruncatedFiles()
      throws IOException {
    BufferedReader reader = new BufferedReader(new StringReader("# comment\n\n{\"resultTable\":{}}\n"));
    assertEquals(QueryOp.readNextExpectedResult(reader, 21), "{\"resultTable\":{}}");
    assertThrows(IOException.class, () -> QueryOp.readNextExpectedResult(reader, 22));
  }

  @Test
  public void testNormalResultComparisonUsesSelectedEngine()
      throws Exception {
    JsonNode actualSubset = successfulResponse(1, 1);
    JsonNode expectedSuperset = JsonUtils.stringToJsonNode(
        "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"EXPR$0\"],\"columnDataTypes\":[\"LONG\"]},"
            + "\"rows\":[[1],[2]]},\"exceptions\":[],\"numDocsScanned\":1,\"isSuperset\":true}");
    String query = "SELECT COUNT(*) FROM testTable";

    assertTrue(QueryOp.matchesExpectedResponse(actualSubset, expectedSuperset, null, false, query));
    assertFalse(QueryOp.matchesExpectedResponse(actualSubset, expectedSuperset, null, true, query));

    JsonNode moreExpensiveActual = successfulResponse(1, 2);
    JsonNode expected = successfulResponse(1, 1);
    assertFalse(QueryOp.matchesExpectedResponse(moreExpensiveActual, expected, null, false, query));
    assertTrue(QueryOp.matchesExpectedResponse(moreExpensiveActual, expected, null, true, query));
  }

  @Test
  public void testNullHandlingQueryOptionsAreOptIn() {
    assertEquals(Utils.getSingleStageQueryOptions(false), "groupByMode=sql;responseFormat=sql");
    assertEquals(Utils.getSingleStageQueryOptions(true),
        "groupByMode=sql;responseFormat=sql;enableNullHandling=true");
    assertEquals(Utils.getMultiStageQueryOptions(false), "useMultistageEngine=true");
    assertEquals(Utils.getMultiStageQueryOptions(true), "useMultistageEngine=true;enableNullHandling=true");
  }

  @Test
  public void testNullHandlingYamlPropertyUsesBooleanNaming() {
    Representer representer = new Representer(new DumperOptions());
    representer.getPropertyUtils().setSkipMissingProperties(true);
    Yaml yaml = new Yaml(new CompatibilityOpsRunner.CustomConstructor(new LoaderOptions()), representer);

    CompatTestOperation operation = yaml.loadAs("""
        description: Test null handling property
        operations:
          - type: queryOp
            queryFileName: queries/test.queries
            expectedResultsFileName: query-results/test.results
            nullHandlingEnabled: true
        """, CompatTestOperation.class);
    QueryOp queryOp = (QueryOp) operation.getOperations().get(0);

    assertTrue(queryOp.isNullHandlingEnabled());
  }

  private static JsonNode successfulResponse(int value, int numDocsScanned)
      throws Exception {
    return JsonUtils.stringToJsonNode(
        "{\"resultTable\":{\"dataSchema\":{\"columnNames\":[\"EXPR$0\"],\"columnDataTypes\":[\"LONG\"]},"
            + "\"rows\":[[" + value + "]]},\"exceptions\":[],\"numDocsScanned\":" + numDocsScanned + "}");
  }
}
