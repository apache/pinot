package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.util.QueryUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
public class MetricsResource
{
  private final StarTreeManager starTreeManager;

  public MetricsResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}/{start}/{end}")
  @Timed
  public List<ThirdEyeMetrics> getMetricsInRange(
          @PathParam("collection") String collection,
          @PathParam("start") Long start,
          @PathParam("end") Long end,
          @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result = QueryUtils.doQuery(starTree, start, end, uriInfo);

    return convert(starTree.getConfig(), result);
  }

  @GET
  @Path("/{collection}/{start}/{end}/{movingAverageWindow}")
  @Timed
  public List<ThirdEyeMetrics> getMetricsInRangeWithMovingAverage(
          @PathParam("collection") String collection,
          @PathParam("start") Long start,
          @PathParam("end") Long end,
          @PathParam("movingAverageWindow") Long movingAverageWindow,
          @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result
            = QueryUtils.doQuery(starTree, start - movingAverageWindow, end, uriInfo);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      result.put(entry.getKey(), MetricTimeSeriesUtils.getSimpleMovingAverage(entry.getValue(), start, end, movingAverageWindow));
    }

    return convert(starTree.getConfig(), result);
  }

  private static List<ThirdEyeMetrics> convert(StarTreeConfig config, Map<DimensionKey, MetricTimeSeries> original)
  {
    List<ThirdEyeMetrics> result = new ArrayList<ThirdEyeMetrics>(original.size());

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : original.entrySet())
    {
      ThirdEyeMetrics resultPart = new ThirdEyeMetrics();
      resultPart.setDimensionValues(QueryUtils.convertDimensionKey(config.getDimensions(), entry.getKey()));
      resultPart.setMetricValues(QueryUtils.convertMetricValues(config.getMetrics(), entry.getValue()));
      result.add(resultPart);
    }

    return result;
  }
}
