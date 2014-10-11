package com.linkedin.pinot.controller.api.reslet.resources;

import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    manager = (PinotHelixResourceManager) getApplication().getContext().getAttributes().get(PinotHelixResourceManager.class.toString());
    mapper = new ObjectMapper();
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      final String segmentName = (String) getRequest().getAttributes().get("segmentName");
      if (resourceName != null && segmentName == null) {
        final JSONArray segmentsArray = new JSONArray();
        final JSONObject ret = new JSONObject();
        ret.put("resource", resourceName);

        for (final String segmentId : manager.getAllSegmentsForResource(resourceName)) {
          segmentsArray.put(segmentId);
        }

        ret.put("segments", segmentsArray);
        presentation = new StringRepresentation(ret.toString());

      } else {
        final Map<String, String> segmentMetadata = manager.getMetadataFor(resourceName, segmentName);

        final JSONObject medata = new JSONObject();
        for (final String key : segmentMetadata.keySet()) {
          medata.put(key, segmentMetadata.get(key));
        }

        final JSONObject ret = new JSONObject();
        ret.put("segmentName", segmentName);
        ret.put("metadata", medata);
        presentation = new StringRepresentation(ret.toString());
      }
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }

}
