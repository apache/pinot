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

import java.util.Random;


public class JsonGenerator implements Generator {
  // Length of each key-value pair int the JSON string.
  static final int DEFAULT_JSON_ELEMENT_LENGTH = 5;

  private final int _jsonStringLength;
  private final Random _random;

  public JsonGenerator(Integer jsonSize) {
    _jsonStringLength = jsonSize == null || jsonSize <= 0 ? 0 : jsonSize;
    _random = new Random(System.currentTimeMillis());
  }

  @Override
  public void init() {
  }

  @Override
  public Object next() {
    // Return empty string if size of json string is zero or if number of elements is zero.
    if (_jsonStringLength == 0 || _jsonStringLength / DEFAULT_JSON_ELEMENT_LENGTH == 0) {
      return "{}";
    }

    // Create JSON string { "<character>":<integer>, "<character>":<integer>, ...} as per length specified. Escape
    // comma's in JSON since comma is used as delimiter in CSV data file that will be used to generate segment.
    StringBuffer jsonBuffer = new StringBuffer();
    int elementCount = _jsonStringLength / DEFAULT_JSON_ELEMENT_LENGTH;
    jsonBuffer.append("{");
    for (int i = 0; i < elementCount; i++) {
      if (jsonBuffer.length() > 1) {
        jsonBuffer.append("\\,");
      }
      String item = "\"" + (char) ('a' + _random.nextInt(26)) + "\":" + _random.nextInt(10);
      jsonBuffer.append(item);
    }

    return jsonBuffer.append("}").toString();
  }
}
