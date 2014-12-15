package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.ThirdEyeConstants;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.ThirdEyeMetrics;
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
import java.util.HashSet;
import java.util.List;
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

    return queryTree(starTree, ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo).setTimeRange(start, end).build(), uriInfo);
  }

  @GET
  @Path("/{collection}/{timeBuckets}")
  public List<ThirdEyeMetrics> getMetricsInTimeBuckets(@PathParam("collection") String collection,
                                                       @PathParam("timeBuckets") String timeBuckets,
                                                       @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Set<Long> times = new HashSet<Long>();
    String[] tokens = timeBuckets.split(ThirdEyeConstants.TIME_SEPARATOR);
    for (String token : tokens)
    {
      times.add(Long.valueOf(token));
    }

    return queryTree(starTree, ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo).setTimeBuckets(times).build(), uriInfo);
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

    return queryTree(starTree, ThirdEyeUriUtils.createQueryBuilder(starTree, uriInfo).build(), uriInfo);
  }

  /**
   * Queries tree and converts to JSON result
   */
  private static List<ThirdEyeMetrics> queryTree(StarTree starTree, StarTreeQuery baseQuery, UriInfo uriInfo)
  {
    // Generate queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery);

    // Filter queries
    queries = StarTreeUtils.filterQueries(queries, uriInfo.getQueryParameters());

    // Query tree
    List<ThirdEyeMetrics> metricsResults = new ArrayList<ThirdEyeMetrics>();
    for (StarTreeQuery query : queries)
    {
      StarTreeRecord record = starTree.getAggregate(query);
      ThirdEyeMetrics result = new ThirdEyeMetrics();
      result.setDimensionValues(record.getDimensionValues());
      result.setMetricValues(record.getMetricValues());
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
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValues(metrics.getDimensionValues())
            .setMetricValues(metrics.getMetricValues())
            .setTime(time)
            .build();

    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    starTree.add(record);

    return Response.ok().build();
  }
}
