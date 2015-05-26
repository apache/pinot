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
package com.linkedin.pinot.controller.api.reslet.resources.v2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * Sep 29, 2014
 */

public class PinotSegmentResletResource extends ServerResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentResletResource.class);

  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final ObjectMapper mapper;

  public PinotSegmentResletResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager =
        (PinotHelixResourceManager) getApplication().getContext().getAttributes()
            .get(PinotHelixResourceManager.class.toString());
    mapper = new ObjectMapper();
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String tableName = (String) getRequest().getAttributes().get("tableName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");

      if (segmentName != null) {
        // TODO : {here we need to see if this is a realtime segment name then fetch realtime zk metadata}
        OfflineSegmentZKMetadata offlineSegmentZKMetadata =
            ZKMetadataProvider.getOfflineSegmentZKMetadata(manager.getPropertyStore(),
                TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName), segmentName);

        String res = new ObjectMapper().writeValueAsString(offlineSegmentZKMetadata.toMap());
        return new StringRepresentation(res);
      }

      final String grouping = getQueryValue("grouping") == null ? "instances" : "arrivalDay";

      if (!grouping.equals("instances")) {
        throw new RuntimeException("currently only instance grouping is supported");
      }

      JSONArray ret = new JSONArray();
      if (manager.hasRealtimeTable(tableName)) {
        String realtimeTableName = TableNameBuilder.REALTIME_TABLE_NAME_BUILDER.forTable(tableName);
        JSONObject realtime = new JSONObject();
        realtime.put("tableType", "realtime");
        realtime.put("segments",
            new ObjectMapper().writeValueAsString(manager.getInstanceToSegmentsInATableMap(realtimeTableName)));
        ret.put(realtime);
      }

      if (manager.hasOfflineTable(tableName)) {
        String offlineTableName = TableNameBuilder.OFFLINE_TABLE_NAME_BUILDER.forTable(tableName);
        JSONObject offline = new JSONObject();
        offline.put("tableType", "offline");
        offline.put("segments",
            new ObjectMapper().writeValueAsString(manager.getInstanceToSegmentsInATableMap(offlineTableName)));
        ret.put(offline);
      }

      return new StringRepresentation(ret.toString());

    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      LOGGER.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }
}
