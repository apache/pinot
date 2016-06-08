package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.restlet.Response;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePinotControllerRestletResource.class);

  protected static final String TABLE_NAME = "tableName";
  protected static final String TABLE_TYPE = "type";

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

  static enum StateType {
    ENABLE,
    DISABLE,
    DROP
  }

  protected final ControllerConf _controllerConf;
  protected final PinotHelixResourceManager _pinotHelixResourceManager;

  protected final Executor executor;
  protected final HttpConnectionManager connectionManager;

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
    connectionManager = (HttpConnectionManager) appAttributes.get(HttpConnectionManager.class.toString());
    executor = (Executor) appAttributes.get(Executor.class.toString());
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
    return new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
  }
}
