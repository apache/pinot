package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

@Path("/configs")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeConfigsResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeConfigsResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}")
  @Timed
  public Map<String, Object> getConfig(@PathParam("collection") String collection)
  {
    StarTreeConfig config = starTreeManager.getConfig(collection);

    Map<String, Object> result = new HashMap<String, Object>();
    result.put("collection", config.getCollection());
    result.put("dimensionNames", config.getDimensionNames());
    result.put("metricNames", config.getMetricNames());
    result.put("timeColumnName", config.getTimeColumnName());

    return result;
  }
}
