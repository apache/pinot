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
package com.linkedin.pinot.controller.helix.core;

import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Sep 30, 2014
 */

public class PinotResourceManagerResponse {

  public enum STATUS {
    success,
    failure;
  }

  public String errorMessage = "DEFAULT_VAL";
  public STATUS status = STATUS.failure;
  public Map<String, String> metadata;

  public boolean isSuccessfull() {
    return status == STATUS.success ? true : false;
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("status", status.toString());
    if (status == STATUS.success) {
      ret.put("message", errorMessage);
    } else {
      ret.put("errorMessage", errorMessage);
    }
    if (metadata != null) {
      for (final String key : metadata.keySet()) {
        ret.put(key, metadata.get(key));
      }
    }
    return ret;
  }

  @Override
  public String toString() {
    if (status == STATUS.success) {
      return "status : " + status + ",\tmessage : " + errorMessage;
    } else {
      return "status : " + status + ",\terrorMessage : " + errorMessage;
    }
  }
}
