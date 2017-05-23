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

import org.json.JSONObject;


public class DateFormat extends JSONObject {
  private static String _simpleDate;

  public DateFormat(String simpleDate) {
    _simpleDate = simpleDate;
  }

  public String getSimpleDate() {
    return _simpleDate;
  }

  public void setSimpleDate(String simpleDate) {
    _simpleDate = simpleDate;
  }
}
