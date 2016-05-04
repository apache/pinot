package com.linkedin.thirdeye.dashboard.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

import com.linkedin.thirdeye.dashboard.views.DefaultView;

import io.dropwizard.views.View;

@Path(value = "/")
public class DefaultResource {

  public DefaultResource() {
  }

  @GET
  public View sayHello() {
    return new DefaultView("home");
  }

}
