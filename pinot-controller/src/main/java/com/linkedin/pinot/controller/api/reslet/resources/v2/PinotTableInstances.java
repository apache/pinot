package com.linkedin.pinot.controller.api.reslet.resources.v2;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.reslet.resources.PinotFileUpload;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


public class PinotTableInstances extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableInstances.class);
  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableInstances() throws IOException {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    baseDataDir = new File(conf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;

    final String tableName = (String) getRequest().getAttributes().get("tableName");
    final String type = getQueryValue("type");
    if (tableName == null) {
      return new StringRepresentation("tableName is not present");
    }

    JSONObject ret = new JSONObject();
    try {
      ret.put("tableName", tableName);
      JSONArray brokers = new JSONArray();
      JSONArray servers = new JSONArray();

      if (type == null || type.toLowerCase().equals("broker")) {

        if (manager.hasOfflineTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          e.put(
              "instances",
              new JSONObject(new ObjectMapper().writeValueAsString(manager.getBrokerInstancesForTable(tableName,
                  TableType.OFFLINE))));
          brokers.add(e);
        }

        if (manager.hasRealtimeTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          e.put(
              "instances",
              new JSONObject(new ObjectMapper().writeValueAsString(manager.getBrokerInstancesForTable(tableName,
                  TableType.REALTIME))));
          brokers.add(e);
        }
      }

      if (type == null || type.toLowerCase().equals("server")) {
        if (manager.hasOfflineTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          e.put(
              "instances",
              new JSONObject(new ObjectMapper().writeValueAsString(manager.getServerInstancesForTable(tableName,
                  TableType.OFFLINE))));
          servers.add(e);
        }

        if (manager.hasRealtimeTable(tableName)) {
          JSONObject e = new JSONObject();
          e.put("tableType", "offline");
          e.put(
              "instances",
              new JSONObject(new ObjectMapper().writeValueAsString(manager.getServerInstancesForTable(tableName,
                  TableType.REALTIME))));
          servers.add(e);
        }
      }

      ret.put("brokers", brokers);
      ret.put("server", servers);
      return new StringRepresentation(ret.toString());
    } catch (Exception e) {
      LOGGER.error("error processing fetch table request, ", e);
      return PinotFileUpload.exceptionToStringRepresentation(e);
    }

    // get all instances for this tableName
    return presentation;
  }
}
