package com.linkedin.thirdeye.dashboard.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.linkedin.thirdeye.dashboard.views.ThirdEyeView;

import io.dropwizard.views.View;

@Path(value = "/thirdeye")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeResource {

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public View getDashboardView() {
    return new ThirdEyeView();
  }
}

