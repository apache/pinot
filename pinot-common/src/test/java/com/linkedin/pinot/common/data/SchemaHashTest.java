/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.data;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.Test;


public class SchemaHashTest {

  private String makeSchema() {
    return "{" +
        "\"schemaName\":\"TestSchema\"," +
        "   \"metricFieldSpecs\":[" +
        "{\"dataType\": \"LONG\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"volume\", \"fieldType\":\"METRIC\"}" +
        "    ], \"dimensionFieldSpecs\":[" +
        "{\"dataType\": \"STRING\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"strName\", \"fieldType\":\"DIMENSION\"}" + "," +
        "{\"dataType\": \"LONG\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"longName\", \"fieldType\":\"DIMENSION\"}" + "," +
        "{\"dataType\": \"SHORT\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"shortName\", \"fieldType\":\"DIMENSION\"}" + "," +
        "{\"dataType\": \"BYTE\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"byteName\", \"fieldType\":\"DIMENSION\"}" + "," +
        "{\"dataType\": \"INT\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"intName\", \"fieldType\":\"DIMENSION\"}" + "," +
        "{\"dataType\": \"BOOLEAN\", \"singleValueField\": true, \"delimiter\":\",\", \"name\":\"boolName\", \"fieldType\":\"DIMENSION\"}" +
        "    ], \"timeFieldSpec\":{" +
        "       \"dataType\":\"LONG\"," +
        "       \"incomingGranularitySpec\":{" +
        "          \"dataType\":\"LONG\", \"timeType\":\"MILLISECONDS\", \"name\":\"tick\"" +
        "        }, \"outgoingGranularitySpec\":{" +
        "          \"dataType\":\"LONG\", \"timeType\":\"MILLISECONDS\", \"name\":\"tick\"" +
        "        }," +
        "        \"singleValueField\":true, \"delimiter\":null, \"name\":\"time\", \"fieldType\":\"TIME\", \"defaultNullValue\":-9223372036854775808"
        +
        "    }" +
        " }";
  }

  /*
   * Schema is a member of KafkaHighLevelStreamProviderConfig, which occurs as a map key.
   * Schema expects to hash FieldSpec correctly, which in turn expects getDefautlValue() to work right for hashcode.
   * This test makes sure that all primitive types are covered for this use case.
   */
  @Test
  public void test1() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String schemaStr = makeSchema();
    Schema schema = mapper.readValue(schemaStr, Schema.class);
    schema.hashCode();
  }

}
