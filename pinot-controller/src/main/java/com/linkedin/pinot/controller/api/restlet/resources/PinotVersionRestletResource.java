package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import org.json.JSONObject;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Get;


/**
 * API endpoint that returns the versions of Pinot components.
 */
public class PinotVersionRestletResource extends BasePinotControllerRestletResource {
  @Override
  @Get
  public Representation get() {
    return buildVersionResponse();
  }

  @HttpVerb("get")
  @Summary("Obtains the version number of the Pinot components")
  @Tags({ "version" })
  @Paths({ "/version" })
  private Representation buildVersionResponse() {
    JSONObject jsonObject = new JSONObject(Utils.getComponentVersions());
    return new StringRepresentation(jsonObject.toString(), MediaType.APPLICATION_JSON);
  }
}
