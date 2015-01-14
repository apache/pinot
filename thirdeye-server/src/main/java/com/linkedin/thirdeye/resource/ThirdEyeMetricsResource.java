package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.ThirdEyeConstants;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.util.ThirdEyeUriUtils;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/metrics")
@Produces(MediaType.APPLICATION_JSON)
public class ThirdEyeMetricsResource
{
  private final StarTreeManager starTreeManager;

  public ThirdEyeMetricsResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  // Read

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

    return queryTree(starTree, ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo)
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

    return queryTree(starTree, ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo).build(starTree.getConfig()), uriInfo);
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

  // Write

  @POST
  @Path("/{collection}/{time}")
  @Timed
  public Response postMetrics(@PathParam("collection") String collection,
                              @PathParam("time") Long time,
                              ThirdEyeMetrics metrics)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    String[] dimensionValues = new String[starTree.getConfig().getDimensions().size()];

    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++)
    {
      String dimensionName = starTree.getConfig().getDimensions().get(i).getName();
      dimensionValues[i] = metrics.getDimensionValues().get(dimensionName);
    }

    MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(starTree.getConfig().getMetrics()));
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      timeSeries.set(time, metricSpec.getName(), metrics.getMetricValues().get(metricSpec.getName()));
    }

    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionKey(new DimensionKey(dimensionValues))
            .setMetricTimeSeries(timeSeries)
            .build(starTree.getConfig());

    starTree.add(record);

    return Response.ok().build();
  }
}
