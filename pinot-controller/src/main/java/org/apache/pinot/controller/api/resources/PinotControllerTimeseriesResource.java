package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.ApiOperation;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/timeseries")
public class PinotControllerTimeseriesResource {
  public static final Logger LOGGER = LoggerFactory.getLogger(PinotControllerTimeseriesResource.class);

  @Inject
  ControllerConf _controllerConf;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("languages")
  @ApiOperation(value = "Get timeseries languages from controller configuration",
      notes = "Get timeseries languages from controller configuration")
  public List<String> getBrokerTimeSeriesLanguages(@Context HttpHeaders headers) {
    try {
      return _controllerConf.getTimeseriesLanguages();
    } catch (Exception e) {
      LOGGER.error("Error fetching timeseries languages from controller configuration", e);
      throw new ControllerApplicationException(LOGGER,
          "Error fetching timeseries languages from controller configuration: " + e.getMessage(),
          Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
