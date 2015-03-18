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
package com.linkedin.pinot.controller.api.reslet.resources;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 29, 2014
 */

public class PinotSegment extends ServerResource {
  private static final Logger logger = Logger.getLogger(PinotDataResource.class);

  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final ObjectMapper mapper;

  public PinotSegment() {
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
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      final String tableName = (String) getRequest().getAttributes().get("tableName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");
      if (resourceName != null && segmentName == null) {
        final JSONArray segmentsArray = new JSONArray();
        final JSONObject ret = new JSONObject();
        ret.put("resource", resourceName);

        if (tableName == null) {
          for (final String segmentId : manager.getAllSegmentsForResource(resourceName)) {
            segmentsArray.put(segmentId);
          }
        } else {
          for (final String segmentId : manager.getAllSegmentsForTable(resourceName, tableName)) {
            segmentsArray.put(segmentId);
          }
        }

        ret.put("segments", segmentsArray);
        presentation = new StringRepresentation(ret.toString());

      } else {
        final JSONObject medata = new JSONObject();

        try {
          OfflineSegmentZKMetadata offlineSegmentZKMetadata =
              ZKMetadataProvider.getOfflineSegmentZKMetadata(manager.getPropertyStore(),
                  BrokerRequestUtils.getOfflineResourceNameForResource(resourceName), segmentName);
          for (final String key : offlineSegmentZKMetadata.toMap().keySet()) {
            medata.put(key, offlineSegmentZKMetadata.toMap().get(key));
          }
          final JSONObject ret = new JSONObject();
          ret.put("segmentName", segmentName);
          ret.put("segmentType", "OFFLINE");
          ret.put("metadata", medata);
          presentation = new StringRepresentation(ret.toString());
        } catch (Exception e) {
          presentation = null;
        }

        try {
          if (presentation == null) {
            RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
                ZKMetadataProvider.getRealtimeSegmentZKMetadata(manager.getPropertyStore(),
                    BrokerRequestUtils.getRealtimeResourceNameForResource(resourceName), segmentName);
            for (final String key : realtimeSegmentZKMetadata.toMap().keySet()) {
              medata.put(key, realtimeSegmentZKMetadata.toMap().get(key));
            }
            final JSONObject ret = new JSONObject();
            ret.put("segmentName", segmentName);
            ret.put("segmentType", "REALTIME");
            ret.put("metadata", medata);
            presentation = new StringRepresentation(ret.toString());
          }
        } catch (Exception e) {
          throw new RuntimeException("Cannot get matched segment from realtime and offline data resource!", e);
        }

      }
    } catch (final Exception e) {
      presentation = new StringRepresentation(e.getMessage() + "\n" + ExceptionUtils.getStackTrace(e));
      logger.error("Caught exception while processing get request", e);
      setStatus(Status.SERVER_ERROR_INTERNAL);
    }
    return presentation;
  }

}
