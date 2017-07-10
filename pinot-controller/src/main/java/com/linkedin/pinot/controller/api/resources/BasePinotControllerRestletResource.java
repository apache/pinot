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
package com.linkedin.pinot.controller.api.resources;

import com.alibaba.fastjson.JSONObject;
import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import com.linkedin.pinot.controller.helix.core.minion.PinotTaskManager;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.engine.header.Header;
import org.restlet.engine.header.HeaderConstants;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;
import org.restlet.util.Series;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for Controller restlet resource apis.
 */
public class BasePinotControllerRestletResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      BasePinotControllerRestletResource.class);

  protected static final String TABLE_NAME = "tableName";
  protected static final String TABLE_TYPE = "type";

  protected static final String TASK_NAME = "taskName";
  protected static final String TASK_TYPE = "taskType";

  protected static final String INSTANCE_NAME = "instanceName";
  protected static final String SEGMENT_NAME = "segmentName";
  protected static final String STATE = "state";

  protected final static String INVALID_STATE_ERROR = "Error: Invalid state, must be one of {enable|disable|drop}'.";
  protected final static String INVALID_INSTANCE_URI_ERROR = "Incorrect URI: must be of form: /instances/{instanceName}[?{state}={enable|disable|drop}]";
  protected final static String INVALID_TABLE_TYPE_ERROR =
      "Error: Invalid table type, must be one of {offline|realtime}'.";
  private final static String HDR_CONTROLLER_HOST = "Pinot-Controller-Host";
  private static String controllerHostName = null;

  private final static String HDR_CONTROLLER_VERSION = "Pinot-Controller-Version";
  private final static String CONTROLLER_COMPONENT = "pinot-controller";
  private static String controllerVersion =  null;

  enum StateType {
    ENABLE,
    DISABLE,
    DROP
  }

  protected final ControllerConf _controllerConf;
  protected final PinotHelixResourceManager _pinotHelixResourceManager;
  protected final PinotHelixTaskResourceManager _pinotHelixTaskResourceManager;
  protected final PinotTaskManager _pinotTaskManager;
  protected final HttpConnectionManager _connectionManager;
  protected final Executor _executor;

  private static String getHostName() {
    if (controllerHostName != null) {
      return controllerHostName;
    }

    controllerHostName = NetUtil.getHostnameOrAddress();
    if (controllerHostName == null) {
      return "Unknown";
    }

    return controllerHostName;
  }

  private static String getControllerVersion() {
    if (controllerVersion != null) {
      return controllerVersion;
    }
    Map<String, String> versions = Utils.getComponentVersions();
    controllerVersion = versions.get(CONTROLLER_COMPONENT);
    if (controllerVersion == null) {
      controllerVersion = "Unknown";
    }
    return controllerVersion;
  }

  public BasePinotControllerRestletResource() {
    ConcurrentMap<String, Object> appAttributes = getApplication().getContext().getAttributes();

    _controllerConf = (ControllerConf) appAttributes.get(ControllerConf.class.toString());
    _pinotHelixResourceManager =
        (PinotHelixResourceManager) appAttributes.get(PinotHelixResourceManager.class.toString());
    _pinotHelixTaskResourceManager =
        (PinotHelixTaskResourceManager) appAttributes.get(PinotHelixTaskResourceManager.class.toString());
    _pinotTaskManager = (PinotTaskManager) appAttributes.get(PinotTaskManager.class.toString());
    _connectionManager = (HttpConnectionManager) appAttributes.get(HttpConnectionManager.class.toString());
    _executor = (Executor) appAttributes.get(Executor.class.toString());
  }

  /**
   * Check if state is one of {enable|disable|drop}
   * @param state; State string to be checked
   * @return True if state is one of {enable|disable|drop}, false otherwise.
   */
  protected static boolean isValidState(String state) {
    return (StateType.ENABLE.name().equalsIgnoreCase(state) ||
        StateType.DISABLE.name().equalsIgnoreCase(state) ||
        StateType.DROP.name().equalsIgnoreCase(state));
  }

  /**
   * Check if the tableType is one of {offline|realtime}
   * @param tableType: Table type string to be checked.
   * @return True if tableType is one of {offilne|realtime}, false otherwise.
   */
  protected static boolean isValidTableType(String tableType) {
    return (TableType.OFFLINE.name().equalsIgnoreCase(tableType) || TableType.REALTIME.name().equalsIgnoreCase(
        tableType));
  }

  public static void addExtraHeaders(Response response) {
    Series<Header> responseHeaders = (Series<Header>)response.getAttributes().get(HeaderConstants.ATTRIBUTE_HEADERS);
    if (responseHeaders == null) {
      responseHeaders = new Series(Header.class);
      response.getAttributes().put(HeaderConstants.ATTRIBUTE_HEADERS, responseHeaders);
    }

    responseHeaders.add(new Header(HDR_CONTROLLER_HOST, getHostName()));
    responseHeaders.add(new Header(HDR_CONTROLLER_VERSION, getControllerVersion()));
  }

  public static StringRepresentation exceptionToStringRepresentation(Exception e) {
    String errorMsg = e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e);
    JSONObject errorMsgInJson = getErrorMsgInJson(errorMsg);
    return new StringRepresentation(errorMsgInJson.toJSONString(), MediaType.APPLICATION_JSON);
  }

  protected static JSONObject getErrorMsgInJson(String errorMsg) {
    JSONObject errorMsgJson = new JSONObject();
    errorMsgJson.put("ERROR", errorMsg);
    return errorMsgJson;
  }

  protected StringRepresentation responseRepresentation(Status status, String jsonMsg) {
    setStatus(status);
    StringRepresentation repr = new StringRepresentation(jsonMsg);
    repr.setMediaType(MediaType.APPLICATION_JSON);
    return repr;
  }

  protected StringRepresentation errorResponseRepresentation(Status status, String msg) {
    return responseRepresentation(status, "{\"error\" : \"" + msg + "\"}");
  }
}
