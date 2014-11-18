package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.ThirdEyeConstants;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.hibernate.validator.constraints.NotEmpty;

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
  public List<Result> getMetricsInRange(@PathParam("collection") String collection,
                                        @PathParam("start") Long start,
                                        @PathParam("end") Long end,
                                        @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    return queryTree(starTree, createQueryBuilder(starTree, uriInfo).setTimeRange(start, end).build());
  }

  @GET
  @Path("/{collection}/{timeBuckets}")
  public List<Result> getMetricsInTimeBuckets(@PathParam("collection") String collection,
                                              @PathParam("timeBuckets") String timeBuckets,
                                              @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    Set<Long> times = new HashSet<Long>();
    String[] tokens = timeBuckets.split(ThirdEyeConstants.TIME_SEPARATOR);
    for (String token : tokens)
    {
      times.add(Long.valueOf(token));
    }

    return queryTree(starTree, createQueryBuilder(starTree, uriInfo).setTimeBuckets(times).build());
  }

  @GET
  @Path("/{collection}")
  @Timed
  public List<Result> getMetrics(@PathParam("collection") String collection,
                                 @Context UriInfo uriInfo)
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    return queryTree(starTree, createQueryBuilder(starTree, uriInfo).build());
  }

  /**
   * Creates a getAggregate builder and sets the dimension values as those from URI getAggregate string
   */
  private static StarTreeQueryImpl.Builder createQueryBuilder(StarTree starTree, UriInfo uriInfo)
  {
    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();
    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      String dimensionValue = uriInfo.getQueryParameters().getFirst(dimensionName);
      if (dimensionValue == null)
      {
        dimensionValue = StarTreeConstants.STAR;
      }
      queryBuilder.setDimensionValue(dimensionName, dimensionValue);
    }
    return queryBuilder;
  }

  /**
   * Queries tree and converts to JSON result
   */
  private static List<Result> queryTree(StarTree starTree, StarTreeQuery baseQuery)
  {
    // Generate queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, baseQuery, true); // roll up

    // Query tree
    List<Result> metricsResults = new ArrayList<Result>();
    for (StarTreeQuery query : queries)
    {
      StarTreeRecord record = starTree.getAggregate(query);
      Result result = new Result();
      result.setDimensionValues(record.getDimensionValues());
      result.setMetricValues(record.getMetricValues());
      metricsResults.add(result);
    }

    return metricsResults;
  }

  public static class Result
  {
    @NotEmpty
    private Map<String, String> dimensionValues;

    @NotEmpty
    private Map<String, Integer> metricValues;

    @JsonProperty
    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    @JsonProperty
    public void setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues = dimensionValues;
    }

    @JsonProperty
    public Map<String, Integer> getMetricValues()
    {
      return metricValues;
    }

    @JsonProperty
    public void setMetricValues(Map<String, Integer> metricValues)
    {
      this.metricValues = metricValues;
    }
  }

  // Write

  @POST
  @Path("/{collection}")
  @Timed
  public Response postMetrics(@PathParam("collection") String collection, Payload payload)
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValues(payload.getDimensionValues())
            .setMetricValues(payload.getMetricValues())
            .setTime(payload.getTime())
            .build();

    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    starTree.add(record);

    return Response.ok().build();
  }

  public static class Payload
  {
    @NotEmpty
    private Map<String, String> dimensionValues;

    @NotEmpty
    private Map<String, Integer> metricValues;

    @NotEmpty
    private Long time;

    @JsonProperty
    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    @JsonProperty
    public void setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues = dimensionValues;
    }

    @JsonProperty
    public Map<String, Integer> getMetricValues()
    {
      return metricValues;
    }

    @JsonProperty
    public void setMetricValues(Map<String, Integer> metricValues)
    {
      this.metricValues = metricValues;
    }

    @JsonProperty
    public Long getTime()
    {
      return time;
    }

    @JsonProperty
    public void setTime(Long time)
    {
      this.time = time;
    }
  }
}
