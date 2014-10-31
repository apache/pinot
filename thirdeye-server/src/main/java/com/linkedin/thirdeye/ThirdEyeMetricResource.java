package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;

@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeMetricResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeMetricResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Timed
  public ThirdEyeMetricData getMetricData(@QueryParam("collection") String collection)
  {
    ThirdEyeMetricData data = new ThirdEyeMetricData();
    data.setDimensionValues(new HashMap<String, String>());
    data.setMetricValues(new HashMap<String, Long>());
    return data;
  }
}
