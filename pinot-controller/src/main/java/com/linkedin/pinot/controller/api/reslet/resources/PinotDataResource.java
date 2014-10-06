package com.linkedin.pinot.controller.api.reslet.resources;

import org.apache.log4j.Logger;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.representation.Variant;
import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Post;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.pojos.DataResource;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 *   sample curl call
 *  curl -i -X POST -H 'Content-Type: application/json' -d
    '{"resourceName":"resourceName","tableName":"tableName","timeColumnName":"timeColumnName",
    "timeType":"timeType","numInstances":2,"numReplicas":3,"retentionTimeUnit":"retentionTimeUnit",
    "retentionTimeValue":"retentionTimeValue","pushFrequency":"pushFrequency"}'
    http://localhost:8998/resource
 *
 */

public class PinotDataResource extends ServerResource {
  private static final Logger logger = Logger.getLogger(PinotDataResource.class);

  private final ControllerConf conf;
  private final PinotHelixResourceManager manager;
  private final ObjectMapper mapper;

  public PinotDataResource() {
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
    getVariants().add(new Variant(MediaType.APPLICATION_JSON));
    setNegotiated(false);
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    manager = (PinotHelixResourceManager) getApplication().getContext().getAttributes().get(PinotHelixResourceManager.class.toString());
    mapper = new ObjectMapper();
  }

  @Override
  @Put("json")
  public Representation put(Representation entity) {
    StringRepresentation presentation = null;
    try {
      final DataResource resource = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), DataResource.class);
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      presentation = new StringRepresentation("put request for : " + resourceName);
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }

  @Override
  @Delete
  public Representation delete() {
    StringRepresentation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      presentation = new StringRepresentation(manager.deleteResource(resourceName).toJSON().toString());
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }

  @Override
  @Get
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      final String resourceName = (String) getRequest().getAttributes().get("resourceName");
      presentation = new StringRepresentation(manager.getDataResource(resourceName).toJSON().toString());
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }

  @Override
  @Post("json")
  public Representation post(Representation entity) {
    StringRepresentation presentation = null;
    try {
      final DataResource resource = mapper.readValue(ByteStreams.toByteArray(entity.getStream()), DataResource.class);
      presentation = new StringRepresentation( manager.createDataResource(resource).toJSON().toString());
    } catch (final Exception e) {
      e.printStackTrace();
      presentation = new StringRepresentation(e.getMessage());
      logger.error(e);
    }
    return presentation;
  }

}
