package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.ThirdEyeUriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/timeSeries")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeTimeSeriesResource
{
  private final StarTreeManager manager;

  public ThirdEyeTimeSeriesResource(StarTreeManager manager)
  {
    this.manager = manager;
  }

  @GET
  @Path("/{collection}/{metric}/{start}/{end}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeries(@PathParam("collection") String collection,
                                                @PathParam("metric") String metric,
                                                @PathParam("start") Long start,
                                                @PathParam("end") Long end,
                                                @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }
    if (!starTree.getConfig().getMetricNames().contains(metric))
    {
      throw new NotFoundException("No metric " + metric + " in collection " + collection);
    }

    // Expand queries
    List<StarTreeQuery> queries
            = StarTreeUtils.expandQueries(starTree,
                                          ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo)
                                                          .setTimeRange(start, end).build());

    // Filter queries
    queries = StarTreeUtils.filterQueries(queries, uriInfo.getQueryParameters());

    // Query tree for time series
    List<ThirdEyeTimeSeries> results = new ArrayList<ThirdEyeTimeSeries>(queries.size());
    for (StarTreeQuery query : queries)
    {
      List<StarTreeRecord> timeSeries = starTree.getTimeSeries(query);
      ThirdEyeTimeSeries result = new ThirdEyeTimeSeries();
      result.setMetricName(metric);
      result.setTimeSeries(convertTimeSeries(metric, timeSeries));
      result.setDimensionValues(query.getDimensionValues());
      results.add(result);
    }

    return results;
  }

  private static List<List<Number>> convertTimeSeries(String metric, List<StarTreeRecord> records)
  {
    List<List<Number>> timeSeries = new ArrayList<List<Number>>(records.size());

    for (StarTreeRecord record : records)
    {
      timeSeries.add(Arrays.asList(record.getTime(), record.getMetricValues().get(metric)));
    }

    return timeSeries;
  }
}
