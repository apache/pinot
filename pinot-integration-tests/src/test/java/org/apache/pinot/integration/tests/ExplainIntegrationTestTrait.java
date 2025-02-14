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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.JsonUtils;
import org.intellij.lang.annotations.Language;
import org.testng.Assert;


public interface ExplainIntegrationTestTrait {

  JsonNode postQuery(@Language("sql") String query)
      throws Exception;

  default void explainLogical(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("explain plan without implementation for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      Assert.assertEquals(plan.asText(), expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  default void explainSse(boolean verbose, @Language("sql") String query, Object... expected) {
    try {
      @Language("sql")
      String actualQuery = "SET useMultistageEngine=false; explain plan for " + query;
      if (verbose) {
        actualQuery = "SET explainPlanVerbose=true; " + actualQuery;
      }
      JsonNode jsonNode = postQuery(actualQuery);
      JsonNode plan = jsonNode.get("resultTable").get("rows");
      List<String> planAsStrList = (List<String>) JsonUtils.jsonNodeToObject(plan, List.class).stream()
          .map(Object::toString)
          .collect(Collectors.toList());

      if (planAsStrList.size() != expected.length) {
        Assert.fail("Actual: " + planAsStrList + ", Expected: " + Arrays.toString(expected)
                + ". Size mismatch. Actual: " + planAsStrList.size() + ", Expected: " + expected.length);
      }
      for (int i = 0; i < planAsStrList.size(); i++) {
        String planAsStr = planAsStrList.get(i);
        Object expectedObj = expected[i];
        if (expectedObj instanceof Pattern) {
          Assert.assertTrue(((Pattern) expectedObj).matcher(planAsStr).matches(),
              "Pattern doesn't match. Actual: " + planAsStr + ", Expected: " + expectedObj
              + ", Actual complete plan: " + planAsStrList);
        } else if (expectedObj instanceof String) {
          Assert.assertEquals(planAsStr, expectedObj, "Actual: " + planAsStr + ", Expected: " + expectedObj
            + ", Actual complete plan: " + planAsStrList);
        } else {
          Assert.fail("Expected object should be either Pattern or String in position " + i + ". Actual: "
              + expectedObj + " of type " + expectedObj.getClass());
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  default void explainSse(@Language("sql") String query, Object... expected) {
    explainSse(false, query, expected);
  }

  default void explain(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      Assert.assertEquals(plan.asText(), expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  default void explainAskingServers(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("set explainAskingServers=true; explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      Assert.assertEquals(plan.asText(), expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  default void explainVerbose(@Language("sql") String query, String expected) {
    try {
      JsonNode jsonNode = postQuery("set explainPlanVerbose=true; explain plan for " + query);
      JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

      String actual = plan.asText()
          .replaceAll("numDocs=\\[[^\\]]*]", "numDocs=[any]")
          .replaceAll("segment=\\[[^\\]]*]", "segment=[any]")
          .replaceAll("totalDocs=\\[[^\\]]*]", "totalDocs=[any]");


      Assert.assertEquals(actual, expected);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
