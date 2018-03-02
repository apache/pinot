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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Produces(MediaType.APPLICATION_JSON)
public class DataCompletenessResource {
  private static final Logger LOG = LoggerFactory.getLogger(DataCompletenessResource.class);

  private final DataCompletenessConfigManager dataCompletenessDAO;

  public DataCompletenessResource(DataCompletenessConfigManager dataCompletenessDAO) {
    this.dataCompletenessDAO = dataCompletenessDAO;
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
