package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.UriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
  @Path("/raw/{collection}/{metrics}/{start}/{end}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeries(@PathParam("collection") String collection,
                                                @PathParam("metrics") String metrics,
                                                @PathParam("start") Long start,
                                                @PathParam("end") Long end,
                                                @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    return doQuery(starTree, start, end, metrics.split(","), uriInfo);
  }

  @GET
  @Path("/raw/{collection}/{metrics}/{start}/{end}/{timeWindow}")
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

    List<ThirdEyeTimeSeries> result =  doQuery(starTree, start, end, metrics.split(","), uriInfo);

    List<ThirdEyeTimeSeries> aggregatedResult = new ArrayList<ThirdEyeTimeSeries>(result.size());
    for (ThirdEyeTimeSeries timeSeries : result)
    {
      aggregatedResult.add(aggregate(starTree, timeSeries, timeWindow));
    }

    return aggregatedResult;
  }

  @GET
  @Path("/normalized/{collection}/{metrics}/{start}/{end}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeriesNormalized(@PathParam("collection") String collection,
                                                          @PathParam("metrics") String metrics,
                                                          @PathParam("start") Long start,
                                                          @PathParam("end") Long end,
                                                          @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    List<ThirdEyeTimeSeries> result =  doQuery(starTree, start, end, metrics.split(","), uriInfo);

    normalize(starTree, result);

    return result;
  }

  @GET
  @Path("/normalized/{collection}/{metrics}/{start}/{end}/{timeWindow}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeriesNormalized(@PathParam("collection") String collection,
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

    List<ThirdEyeTimeSeries> result =  doQuery(starTree, start, end, metrics.split(","), uriInfo);

    List<ThirdEyeTimeSeries> aggregatedResult = new ArrayList<ThirdEyeTimeSeries>(result.size());
    for (ThirdEyeTimeSeries timeSeries : result)
    {
      aggregatedResult.add(aggregate(starTree, timeSeries, timeWindow));
    }

    normalize(starTree, aggregatedResult);

    return aggregatedResult;
  }

  private static ThirdEyeTimeSeries aggregate(StarTree starTree, ThirdEyeTimeSeries timeSeries, Long timeWindow)
  {
    Map<Long, Number> aggregates = new HashMap<Long, Number>();

    // Get metric spec
    MetricSpec metricSpec = null;
    for (MetricSpec ms : starTree.getConfig().getMetrics())
    {
      if (ms.getName().equals(timeSeries.getLabel()))
      {
        metricSpec = ms;
        break;
      }
    }
    if (metricSpec == null)
    {
      throw new IllegalStateException("Could not find metric " + timeSeries.getLabel());
    }

    // Aggregate across buckets
    for (List<Number> point : timeSeries.getData())
    {
      Long bucket = (point.get(0).longValue() / timeWindow) * timeWindow;
      Number aggregate = aggregates.get(bucket);
      if (aggregate == null)
      {
        aggregate = 0;
      }
      aggregates.put(bucket, NumberUtils.sum(aggregate, point.get(1), metricSpec.getType()));
    }

    // Get times
    List<Long> times = new ArrayList<Long>(aggregates.keySet());
    Collections.sort(times);

    // Create new object
    ThirdEyeTimeSeries aggregatedTimeSeries = new ThirdEyeTimeSeries();
    aggregatedTimeSeries.setDimensionValues(timeSeries.getDimensionValues());
    aggregatedTimeSeries.setLabel(timeSeries.getLabel());
    for (Long time : times)
    {
      aggregatedTimeSeries.addRecord(time, aggregates.get(time));
    }

    return aggregatedTimeSeries;
  }

  private static void normalize(StarTree starTree, List<ThirdEyeTimeSeries> result)
  {
    for (ThirdEyeTimeSeries timeSeries : result)
    {
      // Get metric spec
      MetricSpec metricSpec = null;
      for (MetricSpec ms : starTree.getConfig().getMetrics())
      {
        if (ms.getName().equals(timeSeries.getLabel()))
        {
          metricSpec = ms;
          break;
        }
      }
      if (metricSpec == null)
      {
        throw new IllegalStateException("Could not find metric " + timeSeries.getLabel());
      }

      // Normalize
      if (!timeSeries.getData().isEmpty())
      {
        // Find first non-zero value
        Double baseline = null;
        for (List<Number> point : timeSeries.getData())
        {
          if (!NumberUtils.isZero(point.get(1), metricSpec.getType()))
          {
            baseline = point.get(1).doubleValue();
            break;
          }
        }

        // Normalize all values to that
        if (baseline != null)
        {
          for (List<Number> point : timeSeries.getData())
          {
            point.set(1, point.get(1).doubleValue() / baseline);
          }
        }
      }
    }
  }

  private static List<ThirdEyeTimeSeries> doQuery(StarTree starTree,
                                                  long start,
                                                  long end,
                                                  String[] metricNames,
                                                  UriInfo uriInfo)
  {
    if (start > end)
    {
      throw new WebApplicationException(new IllegalArgumentException("start > end"), 400);
    }

    // Expand queries
    List<StarTreeQuery> queries
            = StarTreeUtils.expandQueries(starTree,
                                          UriUtils.createQueryBuilder(starTree, uriInfo)
                                                  .setTimeRange(new TimeRange(start, end)).build(starTree.getConfig()));

    // Filter queries
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    List<ThirdEyeTimeSeries> allResults = new ArrayList<ThirdEyeTimeSeries>(queries.size());
    for (StarTreeQuery query : queries)
    {
      // Query tree
      MetricTimeSeries timeSeries = starTree.getTimeSeries(query);

      // Convert dimension values
      Map<String, String> values = new HashMap<String, String>(query.getDimensionKey().getDimensionValues().length);
      for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
      {
        values.put(starTree.getConfig().getDimensions().get(i).getName(),
                   query.getDimensionKey().getDimensionValues()[i]);
      }

      // Initialize result by metric
      Map<String, ThirdEyeTimeSeries> result = new HashMap<String, ThirdEyeTimeSeries>(metricNames.length);
      for (String metricName : metricNames)
      {
        ThirdEyeTimeSeries ts = new ThirdEyeTimeSeries();
        ts.setLabel(metricName);
        ts.setDimensionValues(values);
        result.put(metricName, ts);
      }

      // Add metric data
      List<Long> times = new ArrayList<Long>(timeSeries.getTimeWindowSet());
      Collections.sort(times);
      for (Long time : times)
      {
        for (Map.Entry<String, ThirdEyeTimeSeries> entry : result.entrySet())
        {
          entry.getValue().addRecord(time, timeSeries.get(time, entry.getKey()));
        }
      }

      allResults.addAll(result.values());
    }

    return allResults;
  }
}
