package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;


@Api(tags = "testResource")
@Path("/testResource")
public class DummyTestResource {
  @GET
  @Path("requestFilter")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Test API for table name translation in request filter")
  public Map<String, String> getDummyMsg(@QueryParam("tableName") String tableName,
      @QueryParam("tableNameWithType") String tableNameWithType,
      @QueryParam("schemaName") String schemaName) {
    Map<String, String> ret = new HashMap<>();
    ret.put("tableName", tableName);
    ret.put("tableNameWithType", tableNameWithType);
    ret.put("schemaName", schemaName);
    return ret;
  }
}
