package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.util.ApiUtils;
import com.linkedin.thirdeye.util.QueryUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Path("/timeSeries")
@Produces(MediaType.APPLICATION_JSON)
public class TimeSeriesResource
{
  private final StarTreeManager manager;

  public TimeSeriesResource(StarTreeManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/aggregate/{timeWindow}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeries(@PathParam("collection") String collection,
                                                @PathParam("metrics") String metrics,
                                                @PathParam("start") Long start,
                                                @PathParam("end") Long end,
                                                @PathParam("timeWindow") Long timeWindow,
                                                @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result = QueryUtils.doQuery(starTree, start, end, uriInfo);

    return ApiUtils.convertTimeSeries(starTree.getConfig(), Arrays.asList(metrics.split(",")), timeWindow, result);
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/aggregate/{timeWindow}/normalized")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeriesNormalized(@PathParam("collection") String collection,
                                                          @PathParam("metrics") String metrics,
                                                          @PathParam("start") Long start,
                                                          @PathParam("end") Long end,
                                                          @PathParam("timeWindow") Long timeWindow,
                                                          @Context UriInfo uriInfo)
  {
    return getTimeSeriesNormalized(collection, metrics, start, end, timeWindow, null, uriInfo);
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/aggregate/{timeWindow}/normalized/{normalizingMetricName}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeriesNormalized(@PathParam("collection") String collection,
                                                          @PathParam("metrics") String metrics,
                                                          @PathParam("start") Long start,
                                                          @PathParam("end") Long end,
                                                          @PathParam("timeWindow") Long timeWindow,
                                                          @PathParam("normalizingMetricName") String normalizingMetricName,
                                                          @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result = QueryUtils.doQuery(starTree, start, end, uriInfo);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      MetricTimeSeries normalized = normalizingMetricName == null
              ? MetricTimeSeriesUtils.normalize(entry.getValue())
              : MetricTimeSeriesUtils.normalize(entry.getValue(), normalizingMetricName);

      result.put(entry.getKey(), normalized);
    }

    return ApiUtils.convertTimeSeries(starTree.getConfig(), Arrays.asList(metrics.split(",")), timeWindow, result);
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/movingAverage/{timeWindow}")
  @Timed
  public List<ThirdEyeTimeSeries> getMovingAverage(@PathParam("collection") String collection,
                                                   @PathParam("metrics") String metrics,
                                                   @PathParam("start") Long start,
                                                   @PathParam("end") Long end,
                                                   @PathParam("timeWindow") Long timeWindow,
                                                   @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result
            = QueryUtils.doQuery(starTree, start - timeWindow, end, uriInfo);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      result.put(entry.getKey(), MetricTimeSeriesUtils.getSimpleMovingAverage(entry.getValue(), start, end, timeWindow));
    }

    return ApiUtils.convertTimeSeries(starTree.getConfig(), Arrays.asList(metrics.split(",")), 1L, result);
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/movingAverage/{timeWindow}/normalized")
  @Timed
  public List<ThirdEyeTimeSeries> getNormalizedMovingAverage(@PathParam("collection") String collection,
                                                             @PathParam("metrics") String metrics,
                                                             @PathParam("start") Long start,
                                                             @PathParam("end") Long end,
                                                             @PathParam("timeWindow") Long timeWindow,
                                                             @Context UriInfo uriInfo)
  {
    return getNormalizedMovingAverage(collection, metrics, start, end, timeWindow, null, uriInfo);
  }

  @GET
  @Path("/{collection}/{metrics}/{start}/{end}/movingAverage/{timeWindow}/normalized/{normalizingMetricName}")
  @Timed
  public List<ThirdEyeTimeSeries> getNormalizedMovingAverage(@PathParam("collection") String collection,
                                                             @PathParam("metrics") String metrics,
                                                             @PathParam("start") Long start,
                                                             @PathParam("end") Long end,
                                                             @PathParam("timeWindow") Long timeWindow,
                                                             @PathParam("normalizingMetricName") String normalizingMetricName,
                                                             @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Map<DimensionKey, MetricTimeSeries> result
            = QueryUtils.doQuery(starTree, start - timeWindow, end, uriInfo);

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      MetricTimeSeries movingAverageTimeSeries
              = MetricTimeSeriesUtils.getSimpleMovingAverage(entry.getValue(), start, end, timeWindow);
      MetricTimeSeries normalized = normalizingMetricName == null
              ? MetricTimeSeriesUtils.normalize(movingAverageTimeSeries)
              : MetricTimeSeriesUtils.normalize(movingAverageTimeSeries, normalizingMetricName);
      result.put(entry.getKey(), normalized);
    }

    return ApiUtils.convertTimeSeries(starTree.getConfig(), Arrays.asList(metrics.split(",")), 1L, result);
  }
}
