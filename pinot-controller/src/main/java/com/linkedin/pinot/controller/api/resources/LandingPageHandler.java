package com.linkedin.pinot.controller.api.resources;

import java.io.InputStream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
public class LandingPageHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(LandingPageHandler.class);

  // We configure this static resource as jersey handler because all our APIs are at "/"
  // So, the framework does not serve base index.html page correctly. See ControllerAdminApiApplication
  // for more details.
  @GET
  @Produces(MediaType.TEXT_HTML)
  public InputStream getIndexPage() {
    return getClass().getClassLoader().getResourceAsStream("static/index.html");
  }
}
