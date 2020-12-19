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
package org.apache.pinot.common.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class JsonFunctionsTest {

  @Test
  public void testJsonFunction()
      throws JsonProcessingException {
    String jsonString = "{" +
        "  \"id\": \"7044885078\"," +
        "  \"type\": \"CreateEvent\"," +
        "  \"actor\": {" +
        "    \"id\": 33500718," +
        "    \"login\": \"dipper-github-icn-bom-cdg\"," +
        "    \"display_login\": \"dipper-github-icn-bom-cdg\"," +
        "    \"gravatar_id\": \"\"," +
        "    \"url\": \"https://api.github.com/users/dipper-github-icn-bom-cdg\"," +
        "    \"avatar_url\": \"https://avatars.githubusercontent.com/u/33500718?\"" +
        "  }," +
        "  \"repo\": {" +
        "    \"id\": 112368043," +
        "    \"name\": \"dipper-github-icn-bom-cdg/test-ruby-sample\"," +
        "    \"url\": \"https://api.github.com/repos/dipper-github-icn-bom-cdg/test-ruby-sample\"" +
        "  }," +
        "  \"payload\": {" +
        "    \"ref\": \"canary-test-7f3af0db-3ffa-4259-894f-950d2c76594b\"," +
        "    \"ref_type\": \"branch\"," +
        "    \"master_branch\": \"master\"," +
        "    \"description\": null," +
        "    \"pusher_type\": \"user\"" +
        "  }," +
        "  \"public\": true," +
        "  \"created_at\": \"2018-01-01T11:12:53Z\"" +
        "}";
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.id"), "33500718");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.id"), 33500718L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.id"), 33500718.0);
    assertEquals(JsonFunctions.jsonPathString(jsonString, "$.actor.aaa", "null"), "null");
    assertEquals(JsonFunctions.jsonPathLong(jsonString, "$.actor.aaa", 100L), 100L);
    assertEquals(JsonFunctions.jsonPathDouble(jsonString, "$.actor.aaa", 53.2), 53.2);
  }

}
