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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  @Path("/{collection}/{metrics}/{start}/{end}")
  @Timed
  public List<ThirdEyeTimeSeries> getTimeSeries(@PathParam("collection") String collection,
                                                @PathParam("metrics") String metrics,
                                                @PathParam("start") Long start,
                                                @PathParam("end") Long end,
                                                @Context UriInfo uriInfo)
  {
    String[] metricNames = metrics.split(",");

    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
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
      results.addAll(convertTimeSeries(metricNames, query.getDimensionValues(), timeSeries));
    }

    return results;
  }

  private static List<ThirdEyeTimeSeries> convertTimeSeries(String[] metricNames,
                                                            Map<String, String> dimensionValues,
                                                            List<StarTreeRecord> records)
  {
    Map<String, ThirdEyeTimeSeries> timeSeries = new HashMap<String, ThirdEyeTimeSeries>(metricNames.length);

    for (String metricName : metricNames)
    {
      ThirdEyeTimeSeries ts = new ThirdEyeTimeSeries();
      ts.setLabel(metricName);
      ts.setDimensionValues(dimensionValues);
      timeSeries.put(metricName, ts);
    }

    for (StarTreeRecord record : records)
    {
      for (Map.Entry<String, ThirdEyeTimeSeries> entry : timeSeries.entrySet())
      {
        entry.getValue().addRecord(record.getTime(), record.getMetricValues().get(entry.getKey()));
      }
    }

    return new ArrayList<ThirdEyeTimeSeries>(timeSeries.values());
  }
}
