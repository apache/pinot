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
package com.linkedin.pinot.common.utils;

import com.google.common.collect.Iterators;
import java.util.Iterator;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;


/**
 * Utility class for JSON comparisons in unit tests.
 *
 */
public class JsonAssert {
  /**
   * Compare two JSON objects, ignoring field order. For example, objects {a:1, b:2} and {b:2, a:1} are equals, even
   * though they are not using string comparison.
   *
   * @param actual The actual JSON object
   * @param expected The expected JSON object
   */
  public static void assertEqualsIgnoreOrder(String actual, String expected) {
    try {
      JSONObject actualObject = new JSONObject(actual);
      JSONObject expectedObject = new JSONObject(expected);

      // Check that both objects have the same keys
      Assert.assertTrue(Iterators.elementsEqual(actualObject.sortedKeys(), expectedObject.sortedKeys()),
          "JSON objects don't have the same keys, expected:<" + Iterators.toString(expectedObject.sortedKeys()) +
              "> but was:<" + Iterators.toString(actualObject.sortedKeys()) + ">");

      // Iterate over all the keys of one element and compare their contents
      Iterator<String> objectKeys = actualObject.keys();
      while (objectKeys.hasNext()) {
        String key = objectKeys.next();
        Object actualValue = actualObject.get(key);
        Object expectedValue = expectedObject.get(key);

        Assert.assertTrue(actualValue.getClass().equals(expectedValue.getClass()),
            "Objects with key " + key + " don't have the same class, expected:<" + expectedValue + "> but was:<"
                + actualValue + ">");
        if (actualValue instanceof JSONObject) {
          assertEqualsIgnoreOrder(actualValue.toString(), expectedValue.toString());
        } else {
          Assert.assertEquals(actualValue.toString(), expectedValue.toString(), "Objects with key " + key + " don't have the same value");
        }
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}
