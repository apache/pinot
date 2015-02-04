package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.heatmap.HeatMap;
import com.linkedin.thirdeye.heatmap.HeatMapCell;
import com.linkedin.thirdeye.heatmap.RatioHeatMap;
import com.linkedin.thirdeye.heatmap.SnapshotHeatMap;
import com.linkedin.thirdeye.heatmap.VolumeHeatMap;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.UriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Path("/heatMap")
@Produces(MediaType.APPLICATION_JSON)
public class HeatMapResource
{
  private final StarTreeManager starTreeManager;
  private final ExecutorService parallelQueryExecutor;

  public HeatMapResource(StarTreeManager starTreeManager, ExecutorService parallelQueryExecutor)
  {
    this.starTreeManager = starTreeManager;
    this.parallelQueryExecutor = parallelQueryExecutor;
  }

  @GET
  @Path("/volume/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}")
  @Timed
  public List<HeatMapCell> getVolumeHeatMap(
          @PathParam("collection") String collection,
          @PathParam("metricName") String metricName,
          @PathParam("dimensionName") String dimensionName,
          @PathParam("baselineStart") Long baselineStart,
          @PathParam("baselineEnd") Long baselineEnd,
          @PathParam("currentStart") Long currentStart,
          @PathParam("currentEnd") Long currentEnd,
          @Context UriInfo uriInfo) throws Exception
  {
    return generateHeatMap(new VolumeHeatMap(), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, null, uriInfo);
  }

  @GET
  @Path("/volume/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}/{movingAverageWindow}")
  @Timed
  public List<HeatMapCell> getVolumeHeatMapMovingAverage(
          @PathParam("collection") String collection,
          @PathParam("metricName") String metricName,
          @PathParam("dimensionName") String dimensionName,
          @PathParam("baselineStart") Long baselineStart,
          @PathParam("baselineEnd") Long baselineEnd,
          @PathParam("currentStart") Long currentStart,
          @PathParam("currentEnd") Long currentEnd,
          @PathParam("movingAverageWindow") Long movingAverageWindow,
          @Context UriInfo uriInfo) throws Exception
  {
    return generateHeatMap(new VolumeHeatMap(), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, movingAverageWindow, uriInfo);
  }

  @GET
  @Path("/snapshot/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}")
  @Timed
  public List<HeatMapCell> getSnapshotHeatMap(@PathParam("collection") String collection,
                                              @PathParam("metricName") String metricName,
                                              @PathParam("dimensionName") String dimensionName,
                                              @PathParam("baselineStart") Long baselineStart,
                                              @PathParam("baselineEnd") Long baselineEnd,
                                              @PathParam("currentStart") Long currentStart,
                                              @PathParam("currentEnd") Long currentEnd,
                                              @Context UriInfo uriInfo) throws Exception
  {
    return generateHeatMap(new SnapshotHeatMap(2), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, null, uriInfo);
  }

  @GET
  @Path("/snapshot/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}/{movingAverageWindow}")
  @Timed
  public List<HeatMapCell> getSnapshotHeatMapMovingAverage(
          @PathParam("collection") String collection,
          @PathParam("metricName") String metricName,
          @PathParam("dimensionName") String dimensionName,
          @PathParam("baselineStart") Long baselineStart,
          @PathParam("baselineEnd") Long baselineEnd,
          @PathParam("currentStart") Long currentStart,
          @PathParam("currentEnd") Long currentEnd,
          @PathParam("movingAverageWindow") Long movingAverageWindow,
          @Context UriInfo uriInfo) throws Exception
  {
    return generateHeatMap(new SnapshotHeatMap(2), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, movingAverageWindow, uriInfo);
  }

  @GET
  @Path("/ratio/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}")
  @Timed
  public List<HeatMapCell> getRatioHeatMap(
          @PathParam("collection") String collection,
          @PathParam("metricName") String metricName,
          @PathParam("dimensionName") String dimensionName,
          @PathParam("baselineStart") Long baselineStart,
          @PathParam("baselineEnd") Long baselineEnd,
          @PathParam("currentStart") Long currentStart,
          @PathParam("currentEnd") Long currentEnd,
          @Context UriInfo uriInfo) throws Exception
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    MetricType metricType = null;
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      if (metricSpec.getName().equals(metricName))
      {
        metricType = metricSpec.getType();
        break;
      }
    }

    return generateHeatMap(new RatioHeatMap(metricType), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, null, uriInfo);
  }

  @GET
  @Path("/ratio/{collection}/{metricName}/{dimensionName}/{baselineStart}/{baselineEnd}/{currentStart}/{currentEnd}/{movingAverageWindow}")
  @Timed
  public List<HeatMapCell> getRatioHeatMapMovingAverage(
          @PathParam("collection") String collection,
          @PathParam("metricName") String metricName,
          @PathParam("dimensionName") String dimensionName,
          @PathParam("baselineStart") Long baselineStart,
          @PathParam("baselineEnd") Long baselineEnd,
          @PathParam("currentStart") Long currentStart,
          @PathParam("currentEnd") Long currentEnd,
          @PathParam("movingAverageWindow") Long movingAverageWindow,
          @Context UriInfo uriInfo) throws Exception
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    MetricType metricType = null;
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      if (metricSpec.getName().equals(metricName))
      {
        metricType = metricSpec.getType();
        break;
      }
    }

    return generateHeatMap(new RatioHeatMap(metricType), collection, metricName, dimensionName,
                           baselineStart, baselineEnd, currentStart, currentEnd, movingAverageWindow, uriInfo);
  }

  private List<HeatMapCell> generateHeatMap(
          HeatMap heatMap,
          String collection,
          String metricName,
          String dimensionName,
          Long baselineStart,
          Long baselineEnd,
          Long currentStart,
          Long currentEnd,
          Long movingAverageWindow,
          UriInfo uriInfo) throws Exception
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    TimeRange timeRange = movingAverageWindow != null
            ? new TimeRange(baselineStart - movingAverageWindow, currentEnd)
            : new TimeRange(baselineStart, currentEnd);

    Map<String, MetricTimeSeries> timeSeriesByDimensionValue
            = doQuery(parallelQueryExecutor, starTree, dimensionName, timeRange, uriInfo);

    if (movingAverageWindow != null)
    {
      for (Map.Entry<String, MetricTimeSeries> entry : timeSeriesByDimensionValue.entrySet())
      {
        timeSeriesByDimensionValue.put(
                entry.getKey(),
                MetricTimeSeriesUtils.getSimpleMovingAverage(
                        entry.getValue(), baselineStart, currentEnd, movingAverageWindow));
      }
    }

    return heatMap.generateHeatMap(
            metricName,
            timeSeriesByDimensionValue,
            new TimeRange(baselineStart, baselineEnd),
            new TimeRange(currentStart, currentEnd));
  }

  private static Map<String, MetricTimeSeries> doQuery(ExecutorService parallelQueryExecutor,
                                                       final StarTree starTree,
                                                       String dimensionName,
                                                       TimeRange timeRange,
                                                       UriInfo uriInfo) throws InterruptedException, ExecutionException
  {
    StarTreeQuery baseQuery = UriUtils.createQueryBuilder(starTree, uriInfo).setTimeRange(timeRange).build(starTree.getConfig());

    // Set target dimension to all
    int dimensionIndex = -1;
    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      if (starTree.getConfig().getDimensions().get(i).getName().equals(dimensionName))
      {
        baseQuery.getDimensionKey().getDimensionValues()[i] = StarTreeConstants.ALL;
        dimensionIndex = i;
        break;
      }
    }
    if (dimensionIndex < 0)
    {
      throw new NotFoundException("No dimension " + dimensionName);
    }

    // Generate all queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    // Do queries
    Map<StarTreeQuery, Future<MetricTimeSeries>> futures
            = new HashMap<StarTreeQuery, Future<MetricTimeSeries>>(queries.size());
    for (final StarTreeQuery query : queries)
    {
      futures.put(query, parallelQueryExecutor.submit(new Callable<MetricTimeSeries>()
      {
        @Override
        public MetricTimeSeries call() throws Exception
        {
          return starTree.getTimeSeries(query);
        }
      }));
    }

    // Compose result
    // n.b. all dimension values in results will be distinct because "!" used for query
    Map<String, MetricTimeSeries> result = new HashMap<String, MetricTimeSeries>();
    for (Map.Entry<StarTreeQuery, Future<MetricTimeSeries>> entry : futures.entrySet())
    {
      result.put(entry.getKey().getDimensionKey().getDimensionValues()[dimensionIndex], entry.getValue().get());
    }

    return result;
  }
}
