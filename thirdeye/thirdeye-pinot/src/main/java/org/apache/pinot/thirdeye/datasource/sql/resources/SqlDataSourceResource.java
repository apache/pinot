package org.apache.pinot.thirdeye.datasource.sql.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.thirdeye.datasource.sql.SqlDataset;
import org.apache.pinot.thirdeye.datasource.sql.SqlResponseCacheLoader;
import org.apache.pinot.thirdeye.datasource.sql.SqlUtils;

@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SqlDataSourceResource {

  public SqlDataSourceResource() {}

  @GET
  @Path("/databases")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDatabases() {
    return Response.ok().entity(SqlResponseCacheLoader.getDBNameToURLMap()).build();
  }

  /**
   * POST endpoint that get payload in JSON and onboard the Presto dataset and metrics
   *
   * @param payload Payload in JSON
   *
   * @return Response contains status code 200 or 400
   */
  @POST
  @Path("/onboard")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response onBoard(String payload) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      SqlDataset dataset = mapper.readValue(payload, SqlDataset.class);
      SqlUtils.onBoardSqlDataset(dataset);
      return Response.ok().build();

    } catch (Exception e) {
      System.out.println(e.getMessage());
      return Response
          .status(Response.Status.BAD_REQUEST)
          .build();
    }
  }
}
