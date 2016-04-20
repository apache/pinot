/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.data;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.data.Schema;


public class SchemaTest {
  public static final Logger LOGGER = LoggerFactory.getLogger(SchemaTest.class);

  private static String singleValueDim = "true";
  private static String singleValueMetric = "true";
  private static String dimType = "\"STRING\"";
  private static String metricType = "\"LONG\"";

  private String makeSchema() {
    return "{" +
        "\"schemaName\":\"TestSchema\"," +
        "   \"metricFieldSpecs\":[" +
        "{\"dataType\":" + metricType + ", \"singleValueField\":" + singleValueMetric + ", \"delimiter\":\",\", \"name\":\"volume\", \"fieldType\":\"METRIC\"}" +
        "    ], \"dimensionFieldSpecs\":[" +
        "{\"dataType\":" + dimType + ", \"singleValueField\":" + singleValueDim + ", \"delimiter\":\",\", \"name\":\"page\", \"fieldType\":\"DIMENSION\"}" +
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

  @Test
  public void testValidation() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    {
      singleValueDim = "true";
      singleValueMetric = "true";
      dimType = "\"STRING\"";
      metricType = "\"LONG\"";

      String validSchema = makeSchema();
      Schema schema = mapper.readValue(validSchema, Schema.class);
      Assert.assertTrue(schema.validate(LOGGER));
    }

    {
      singleValueDim = "true";
      singleValueMetric = "false";
      dimType = "\"STRING\"";
      metricType = "\"LONG\"";

      String validSchema = makeSchema();
      Schema schema = mapper.readValue(validSchema, Schema.class);
      Assert.assertTrue(schema.validate(LOGGER));
    }

    {
      singleValueDim = "true";
      singleValueMetric = "true";
      dimType = "\"STRING\"";
      metricType = "\"BOOLEAN\"";

      String validSchema = makeSchema();
      Schema schema = mapper.readValue(validSchema, Schema.class);
      Assert.assertFalse(schema.validate(LOGGER));
    }

    {
      singleValueDim = "true";
      singleValueMetric = "true";
      dimType = "\"STRING\"";
      metricType = "\"STRING\"";

      String validSchema = makeSchema();
      Schema schema = mapper.readValue(validSchema, Schema.class);
      Assert.assertFalse(schema.validate(LOGGER));
    }

    /*
     * Disabled until we fix default value for booleans
    {
      singleValueDim = "true";
      singleValueMetric = "true";
      dimType = "\"BOOLEAN\"";
      metricType = "\"LONG\"";

      String validSchema = makeSchema();
      Schema schema = mapper.readValue(validSchema, Schema.class);
      Assert.assertTrue(schema.validate(LOGGER));
    }

    */
  }
}
