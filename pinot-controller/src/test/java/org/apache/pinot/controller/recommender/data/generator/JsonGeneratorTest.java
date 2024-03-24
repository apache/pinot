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
package org.apache.pinot.controller.recommender.data.generator;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class JsonGeneratorTest {

  @Test
  public void testNext()
      throws IOException {
    // JsonGenerator generates empty json when size is less than JsonGenerator.DEFAULT_JSON_ELEMENT_LENGTH
    JsonGenerator jsonGenerator1 = new JsonGenerator(0);
    Assert.assertEquals(jsonGenerator1.next(), "{}");

    JsonGenerator jsonGenerator2 = new JsonGenerator(4);
    Assert.assertEquals(jsonGenerator2.next(), "{}");

    JsonGenerator jsonGenerator3 = new JsonGenerator(8);
    checkJson((String) jsonGenerator3.next(), 8);

    JsonGenerator jsonGenerator4 = new JsonGenerator(20);
    checkJson((String) jsonGenerator4.next(), 20);
  }

  public static void checkJson(String jsonString, int desiredLength)
      throws IOException {

    // Number of expected elements in the generated JSON string
    int elementCount = desiredLength / JsonGenerator.DEFAULT_JSON_ELEMENT_LENGTH;

    // Remove escape characters from jsonString for verification purposes. Escape character were added before comma
    // since json string is written to a CSV file where comma is used as delimiter.
    jsonString = StringUtils.remove(jsonString, "\\");

    // Make sure we are generating JSON string that is close to the desired length. Length of JSON string should be 2
    // (length of opening and closing parentheses) + number of commas + size of all the elements.
    Assert.assertEquals(jsonString.length(),
        "{}".length() + (elementCount - 1) + elementCount * JsonGenerator.DEFAULT_JSON_ELEMENT_LENGTH);

    // Check if json string is valid json (i.e does not throw parse exceptions) and doesn't result in a
    // JsonNode "missing node".
    JsonNode jsonNode = JsonUtils.stringToJsonNode(jsonString);
    Assert.assertFalse(jsonNode.isMissingNode());
  }
}
