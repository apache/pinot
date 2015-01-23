package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.api.TimeRange;
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
import java.util.ArrayList;
import java.util.HashMap;
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
  public List<ThirdEyeMetrics> getMetricsInRange(@PathParam("collection") String collection,
                                                 @PathParam("start") Long start,
                                                 @PathParam("end") Long end,
                                                 @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    return queryTree(starTree, UriUtils.createQueryBuilder(starTree, uriInfo)
                                               .setTimeRange(new TimeRange(start, end)).build(starTree.getConfig()), uriInfo);
  }

  @GET
  @Path("/{collection}")
  @Timed
  public List<ThirdEyeMetrics> getMetrics(@PathParam("collection") String collection,
                                          @Context UriInfo uriInfo)
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    return queryTree(starTree, UriUtils.createQueryBuilder(starTree, uriInfo).build(starTree.getConfig()), uriInfo);
  }

  /**
   * Queries tree and converts to JSON result
   */
  private static List<ThirdEyeMetrics> queryTree(StarTree starTree, StarTreeQuery baseQuery, UriInfo uriInfo)
  {
    // Generate queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);

    // Filter queries
    queries = StarTreeUtils.filterQueries(starTree.getConfig(), queries, uriInfo.getQueryParameters());

    // Query tree
    List<ThirdEyeMetrics> metricsResults = new ArrayList<ThirdEyeMetrics>();
    for (StarTreeQuery query : queries)
    {
      StarTreeRecord record = starTree.getAggregate(query);

      Map<String, String> dimensionValues = new HashMap<String, String>(starTree.getConfig().getDimensions().size());

      for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
      {
        dimensionValues.put(starTree.getConfig().getDimensions().get(i).getName(),
                            record.getDimensionKey().getDimensionValues()[i]);
      }

      Map<String, Number> metricValues = new HashMap<String, Number>(starTree.getConfig().getMetrics().size());

      Number[] values = record.getMetricTimeSeries().getMetricSums();

      for (int i = 0; i < starTree.getConfig().getMetrics().size(); i++)
      {
        metricValues.put(starTree.getConfig().getMetrics().get(i).getName(), values[i]);
      }

      ThirdEyeMetrics result = new ThirdEyeMetrics();
      result.setDimensionValues(dimensionValues);
      result.setMetricValues(metricValues);
      metricsResults.add(result);
    }
    return metricsResults;
  }
}
