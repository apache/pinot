package org.apache.pinot.server.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.pinot.core.upsert.UpsertMetadataTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = "Debug")
@Path("/serverDebug")
public class DebugResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(DebugResource.class);

  @POST
  @Produces(MediaType.TEXT_PLAIN)
  @Path("/upsert/{enableQuery}")
  @ApiOperation(value = "Debugging upsert query", notes = "")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "Success", response = String.class),
      @ApiResponse(code = 500, message = "Internal server error"),
  })
  public void getUpsertDataAtOffset(
      @ApiParam(value = "enableQuery", required = true, example = "true") @PathParam("enableQuery") boolean enableQuery
  ) {
    UpsertMetadataTableManager.enableUpsertInQuery(enableQuery);
  }
}
