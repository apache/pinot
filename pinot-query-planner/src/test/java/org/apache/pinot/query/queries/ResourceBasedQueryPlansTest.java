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
package org.apache.pinot.query.queries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ResourceBasedQueryPlansTest extends QueryEnvironmentTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String EXPLAIN_REGEX =
      "EXPLAIN (IMPLEMENTATION )*PLAN (INCLUDING |EXCLUDING )*(ALL )*(ATTRIBUTES )*(AS DOT |AS JSON |AS TEXT )*FOR ";
  private static final String QUERY_TEST_RESOURCE_FOLDER = "queries";
  private static final String FILE_FILTER_PROPERTY = "pinot.fileFilter";

  @Test(dataProvider = "testResourceQueryPlannerTestCaseProviderHappyPath")
  public void testQueryExplainPlansAndQueryPlanConversion(String testCaseName, String description, String query,
      String output) {
    try {
      long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
      String explainedPlan = _queryEnvironment.explainQuery(query, requestId);
      Assert.assertEquals(explainedPlan, output,
          String.format("Test case %s for query %s (%s) doesn't match expected output: %s", testCaseName, description,
              query, output));
      // use a regex to exclude the
      String queryWithoutExplainPlan = query.replaceFirst(EXPLAIN_REGEX, "");
      DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(queryWithoutExplainPlan);
      Assert.assertNotNull(dispatchableSubPlan,
          String.format("Test case %s for query %s should not have a null QueryPlan",
              testCaseName, queryWithoutExplainPlan));
    } catch (Exception e) {
      Assert.fail("Test case: " + testCaseName + " failed to explain query: " + query, e);
    }
  }

  @Test(dataProvider = "testResourceQueryPlannerTestCaseProviderExceptions")
  public void testQueryExplainPlansWithExceptions(String testCaseName, String query, String expectedException) {
    try {
      long requestId = RANDOM_REQUEST_ID_GEN.nextLong();
      _queryEnvironment.explainQuery(query, requestId);
      String queryWithoutExplainPlan = query.replaceFirst(EXPLAIN_REGEX, "");
      _queryEnvironment.planQuery(queryWithoutExplainPlan);
      Assert.fail("Query compilation should have failed with exception message pattern: " + expectedException);
    } catch (Exception e) {
      if (expectedException == null) {
        throw e;
      } else {
        Pattern pattern = Pattern.compile(expectedException);
        Assert.assertTrue(pattern.matcher(e.getMessage()).matches(),
            String.format("Caught exception '%s' for test case '%s', but it did not match the expected pattern '%s'.",
                e.getMessage(), testCaseName, expectedException));
      }
    }
  }

  @DataProvider
  private static Object[][] testResourceQueryPlannerTestCaseProviderHappyPath()
      throws Exception {
    Map<String, QueryPlanTestCase> testCaseMap = getTestCases();
    List<Object[]> providerContent = new ArrayList<>();
    for (Map.Entry<String, QueryPlanTestCase> testCaseEntry : testCaseMap.entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      if (testCaseEntry.getValue()._ignored) {
        continue;
      }

      List<QueryPlanTestCase.Query> queryCases = testCaseEntry.getValue()._queries;
      for (QueryPlanTestCase.Query queryCase : queryCases) {
        if (queryCase._ignored || queryCase._expectedException != null) {
          continue;
        }

        if (queryCase._output != null) {
          String sql = queryCase._sql;
          List<String> orgOutput = queryCase._output;
          String concatenatedOutput = StringUtils.join(orgOutput, "");
          Object[] testEntry = new Object[]{testCaseName, queryCase._description, sql, concatenatedOutput};
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  @DataProvider
  private static Object[][] testResourceQueryPlannerTestCaseProviderExceptions()
      throws Exception {
    Map<String, QueryPlanTestCase> testCaseMap = getTestCases();
    List<Object[]> providerContent = new ArrayList<>();
    for (Map.Entry<String, QueryPlanTestCase> testCaseEntry : testCaseMap.entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      if (testCaseEntry.getValue()._ignored) {
        continue;
      }

      List<QueryPlanTestCase.Query> queryCases = testCaseEntry.getValue()._queries;
      for (QueryPlanTestCase.Query queryCase : queryCases) {
        if (queryCase._ignored) {
          continue;
        }

        if (queryCase._expectedException != null) {
          String sql = queryCase._sql;
          String exceptionString = queryCase._expectedException;
          Object[] testEntry = new Object[]{testCaseName, sql, exceptionString};
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  private static Map<String, QueryPlanTestCase> getTestCases()
      throws Exception {
    Map<String, QueryPlanTestCase> testCaseMap = new HashMap<>();
    ClassLoader classLoader = ResourceBasedQueryPlansTest.class.getClassLoader();
    // Get all test files.
    List<String> testFilenames = new ArrayList<>();
    try (InputStream in = classLoader.getResourceAsStream(QUERY_TEST_RESOURCE_FOLDER);
        BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
      String resource;
      while ((resource = br.readLine()) != null) {
        testFilenames.add(resource);
      }
    }

    // get filter if set
    String property = System.getProperty(FILE_FILTER_PROPERTY);

    // Load each test file.
    for (String testCaseName : testFilenames) {
      if (property != null && !testCaseName.toLowerCase().contains(property.toLowerCase())) {
        continue;
      }

      String testCaseFile = QUERY_TEST_RESOURCE_FOLDER + File.separator + testCaseName;
      URL testFileUrl = classLoader.getResource(testCaseFile);
      // This test only supports local resource loading (e.g. must be a file), not support JAR test loading.
      if (testFileUrl != null && new File(testFileUrl.getFile()).exists()) {
        Map<String, QueryPlanTestCase> testCases = MAPPER.readValue(new File(testFileUrl.getFile()),
            new TypeReference<Map<String, QueryPlanTestCase>>() {
            });
        {
          HashSet<String> hashSet = new HashSet<>(testCaseMap.keySet());
          hashSet.retainAll(testCases.keySet());
          if (!hashSet.isEmpty()) {
            throw new IllegalArgumentException("testCase already exist for the following names: " + hashSet);
          }
        }
        testCaseMap.putAll(testCases);
      }
    }
    return testCaseMap;
  }
}
