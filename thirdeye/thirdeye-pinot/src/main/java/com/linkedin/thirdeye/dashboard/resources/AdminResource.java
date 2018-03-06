package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.api.Constants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.linkedin.thirdeye.dashboard.views.ThirdEyeAdminView;

import io.dropwizard.views.View;

@Path(value = "/thirdeye-admin")
@Api(tags = {Constants.ADMIN_TAG } )
@Produces(MediaType.APPLICATION_JSON)
public class AdminResource {

  @GET
  @Path(value = "/")
  @ApiOperation(value = "Load the ThirdEye admin dashboard.")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new ThirdEyeAdminView();
  }
}
