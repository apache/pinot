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
package org.apache.pinot.segment.local.segment.index.json;

import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.spi.utils.JsonUtils;


/// Utility methods shared by JSON index writers and readers.
public final class JsonIndexUtils {
  private JsonIndexUtils() {
  }

  public static boolean isDirectDocIdIndexEligibleKey(String key) {
    return key.isEmpty() || (!key.endsWith(JsonUtils.KEY_SEPARATOR)
        && !key.contains(JsonUtils.KEY_SEPARATOR + JsonUtils.KEY_SEPARATOR)
        && !key.contains(JsonUtils.ARRAY_INDEX_KEY));
  }

  public static boolean isDirectDocIdIndexEligibleValue(String value) {
    int separatorIndex = value.indexOf(JsonIndexCreator.KEY_VALUE_SEPARATOR);
    String key = separatorIndex >= 0 ? value.substring(0, separatorIndex) : value;
    return isDirectDocIdIndexEligibleKey(key);
  }
}
