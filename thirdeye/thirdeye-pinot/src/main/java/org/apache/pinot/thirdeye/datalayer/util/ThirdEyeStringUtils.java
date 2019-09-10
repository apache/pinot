/*
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

package org.apache.pinot.thirdeye.datalayer.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class ThirdEyeStringUtils {
  private static Splitter SEMICOLON_SPLITTER = Splitter.on(";").omitEmptyStrings();
  private static Splitter EQUALS_SPLITTER = Splitter.on("=").omitEmptyStrings();

  private static Joiner SEMICOLON = Joiner.on(";");
  private static Joiner EQUALS = Joiner.on("=");

  /**
   * Encode the properties into String, which is in the format of field1=value1;field2=value2 and so on
   * @param props the properties instance
   * @return a String representation of the given properties
   */
  public static String encodeCompactedProperties(Properties props) {
    List<String> parts = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      parts.add(EQUALS.join(entry.getKey(), entry.getValue()));
    }
    return SEMICOLON.join(parts);
  }

  /**
   * Decode the properties string (e.g. field1=value1;field2=value2) to a properties instance
   * @param propStr a properties string in the format of field1=value1;field2=value2
   * @return a properties instance
   */
  public static Properties decodeCompactedProperties(String propStr) {
    Properties props = new Properties();
    for (String part : SEMICOLON_SPLITTER.split(propStr)) {
      List<String> kvPair = EQUALS_SPLITTER.splitToList(part);
      if (kvPair.size() == 2) {
        props.setProperty(kvPair.get(0).trim(), kvPair.get(1).trim());
      } else if (kvPair.size() == 1) {
        props.setProperty(kvPair.get(0).trim(), "");
      } else {
        throw new IllegalArgumentException(part + " is not a legal property string");
      }
    }
    return props;
  }
}
