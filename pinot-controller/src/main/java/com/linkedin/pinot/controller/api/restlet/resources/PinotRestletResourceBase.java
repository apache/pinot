package com.linkedin.pinot.controller.api.restlet.resources;

import org.restlet.resource.ServerResource;

import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;

/**
 * Base class for Controller restlet resource apis.
 */
public class PinotRestletResourceBase extends ServerResource {

  protected static final String TABLE_NAME = "tableName";
  protected static final String TABLE_TYPE = "type";

  protected static final String INSTANCE_NAME = "instanceName";
  protected static final String SEGMENT_NAME = "segmentName";
  protected static final String STATE = "state";

  protected final static String INVALID_STATE_ERROR = "Error: Invalid state, must be one of {enable|disable|drop}'.";
  protected final static String INVALID_INSTANCE_URI_ERROR = "Incorrect URI: must be of form: /instances/{instanceName}[?{state}={enable|disable|drop}]";
  protected final static String INVALID_TABLE_TYPE_ERROR =
      "Error: Invalid table type, must be one of {offline|realtime}'.";

  static enum StateType {
    ENABLE,
    DISABLE,
    DROP
  }

  protected final ControllerConf _controllerConf;
  protected final PinotHelixResourceManager _pinotHelixResourceManager;

  public PinotRestletResourceBase() {
    _controllerConf =
        (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());

    _pinotHelixResourceManager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
  }

  /**
   * Check if state is one of {enable|disable|drop}
   * @param state; State string to be checked
   * @return True if state is one of {enable|disable|drop}, false otherwise.
   */
  protected static boolean isValidState(String state) {
    return (StateType.ENABLE.name().equalsIgnoreCase(state) || StateType.DISABLE.name().equalsIgnoreCase(state) || StateType.DROP
        .name().equalsIgnoreCase(state));
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
}
