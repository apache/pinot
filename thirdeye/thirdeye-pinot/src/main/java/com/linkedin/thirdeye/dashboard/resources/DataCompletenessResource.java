package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.thirdeye.completeness.checker.DataCompletenessUtils;
import com.linkedin.thirdeye.completeness.checker.PercentCompletenessFunctionInput;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@Path(value = "/data-completeness")
@Produces(MediaType.APPLICATION_JSON)
public class DataCompletenessResource {

  private final DataCompletenessConfigManager dataCompletenessDAO;

  public DataCompletenessResource(DataCompletenessConfigManager dataCompletenessDAO) {
    this.dataCompletenessDAO = dataCompletenessDAO;
  }

  @GET
  @Path(value = "/percent-completeness")
  @Produces(MediaType.APPLICATION_JSON)
  public double getPercentCompleteness(String payload) {
    PercentCompletenessFunctionInput input = PercentCompletenessFunctionInput.fromJson(payload);
    return DataCompletenessUtils.getPercentCompleteness(input);
  }

  @GET
  @Path("/{dataset}/incomplete")
  public Response getDataCompletenessForRangeIncomplete(@PathParam("dataset") String dataset,
      @QueryParam("start") Long start, @QueryParam("end") Long end) throws Exception {
    return makeDataCompletenessResponse(dataset, start, end, false);
  }

  @GET
  @Path("/{dataset}/complete")
  public Response getDataCompletenessForRangeComplete(@PathParam("dataset") String dataset,
      @QueryParam("start") Long start, @QueryParam("end") Long end) throws Exception {
    return makeDataCompletenessResponse(dataset, start, end, true);
  }

  Response makeDataCompletenessResponse(String dataset, long start, long end, boolean complete) throws Exception {
    List<Long> timestamps = getDataCompletenessForRange(dataset, start, end, complete);
    return Response.ok().type("application/json").entity(timestamps).build();
  }

  List<Long> getDataCompletenessForRange(String dataset, long start, long end, boolean complete) {
    List<DataCompletenessConfigDTO> dtos = this.dataCompletenessDAO.findAllByDatasetAndInTimeRangeAndStatus(dataset, start, end, complete);

    List<Long> timestamps = new ArrayList<>();
    for(DataCompletenessConfigDTO dto : dtos) {
      timestamps.add(dto.getDateToCheckInMS());
    }

    return timestamps;
  }

}
