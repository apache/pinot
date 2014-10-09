package com.linkedin.pinot.controller.helix.core;

import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
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
    ret.put("errorMessage", errorMessage);
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
      return "status : " + status;
    } else {
      return "status : " + status + ",\terrorMessage : " + errorMessage;
    }
  }
}
