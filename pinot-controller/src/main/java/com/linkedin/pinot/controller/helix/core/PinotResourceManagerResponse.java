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
package com.linkedin.pinot.controller.helix.core;

import org.json.JSONException;
import org.json.JSONObject;

public class PinotResourceManagerResponse {

  public final static PinotResourceManagerResponse SUCCESS_RESPONSE = new PinotResourceManagerResponse(true);
  public final static PinotResourceManagerResponse FAILURE_RESPONSE = new PinotResourceManagerResponse(false);

  public enum ResponseStatus {
    success,
    failure;
  }

  public String message = "";
  public ResponseStatus status = ResponseStatus.failure;

  public PinotResourceManagerResponse() {
  }

  public PinotResourceManagerResponse(String message, boolean succeeded) {
    this.message = message;
    if (succeeded) {
      status = ResponseStatus.success;
    } else {
      status = ResponseStatus.failure;
    }
  }

  public PinotResourceManagerResponse(boolean isSucceed) {
    if (isSucceed) {
      status = ResponseStatus.success;
    } else {
      status = ResponseStatus.failure;
    }
  }

  public boolean isSuccessful() {
    return status == ResponseStatus.success;
  }

  public JSONObject toJSON() throws JSONException {
    final JSONObject ret = new JSONObject();
    ret.put("status", status.toString());
    if (status == ResponseStatus.success) {
      ret.put("message", message);
    } else {
      ret.put("errorMessage", message);
    }
    return ret;
  }

  @Override
  public String toString() {
    if (status == ResponseStatus.success) {
      return "status : " + status + ",\tmessage : " + message;
    } else {
      return "status : " + status + ",\terrorMessage : " + message;
    }
  }

  public String getMessage() {
    return message;
  }
}
