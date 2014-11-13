package com.linkedin.thirdeye;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
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

  @GET
  @Path("/{collection}")
  @Timed
  public List<Result> getMetrics(@PathParam("collection") String collection, @Context UriInfo uriInfo)
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    StarTreeQueryImpl.Builder queryBuilder = new StarTreeQueryImpl.Builder();

    // Dimension values
    for (String dimensionName : starTree.getConfig().getDimensionNames())
    {
      String dimensionValue = uriInfo.getQueryParameters().getFirst(dimensionName);
      if (dimensionValue == null)
      {
        dimensionValue = StarTreeConstants.STAR;
      }
      queryBuilder.setDimensionValue(dimensionName, dimensionValue);
    }

    // Between, if any
    String betweenClause = uriInfo.getQueryParameters().getFirst(ThirdEyeConstants.BETWEEN);
    if (betweenClause != null)
    {
      String[] tokens = betweenClause.split(ThirdEyeConstants.TIME_SEPARATOR);
      if (tokens.length != 2)
      {
        throw new IllegalArgumentException("BETWEEN must be specified as start,end");
      }
      queryBuilder.setTimeRange(Long.valueOf(tokens[0]), Long.valueOf(tokens[1]));
    }

    // In, if any
    String inClause = uriInfo.getQueryParameters().getFirst(ThirdEyeConstants.IN);
    if (inClause != null)
    {
      String[] tokens = inClause.split(ThirdEyeConstants.TIME_SEPARATOR);
      Set<Long> inSet = new HashSet<Long>();
      for (String token : tokens)
      {
        inSet.add(Long.valueOf(token));
      }
      queryBuilder.setTimeBuckets(inSet);
    }

    // Generate queries
    List<StarTreeQuery> queries = StarTreeUtils.expandQueries(starTree, queryBuilder.build());

    // Query tree
    List<Result> metricsResults = new ArrayList<Result>();
    for (StarTreeQuery query : queries)
    {
      StarTreeRecord record = starTree.query(query);
      Result result = new Result();
      result.setDimensionValues(record.getDimensionValues());
      result.setMetricValues(record.getMetricValues());
      metricsResults.add(result);
    }

    return metricsResults;
  }

  @POST
  @Timed
  public Response postMetrics(Payload payload)
  {
    StarTreeRecord record = new StarTreeRecordImpl.Builder()
            .setDimensionValues(payload.getDimensionValues())
            .setMetricValues(payload.getMetricValues())
            .setTime(payload.getTime())
            .build();

    StarTree starTree = starTreeManager.getStarTree(payload.getCollection());
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + payload.getCollection());
    }

    starTree.add(record);

    return Response.ok().build();
  }

  public static class Payload
  {
    @NotEmpty
    private String collection;

    @NotEmpty
    private Map<String, String> dimensionValues;

    @NotEmpty
    private Map<String, Integer> metricValues;

    @NotEmpty
    private Long time;

    @JsonProperty
    public String getCollection()
    {
      return collection;
    }

    public void setCollection(String collection)
    {
      this.collection = collection;
    }

    @JsonProperty
    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    public void setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues = dimensionValues;
    }

    @JsonProperty
    public Map<String, Integer> getMetricValues()
    {
      return metricValues;
    }

    public void setMetricValues(Map<String, Integer> metricValues)
    {
      this.metricValues = metricValues;
    }

    @JsonProperty
    public Long getTime()
    {
      return time;
    }

    public void setTime(Long time)
    {
      this.time = time;
    }
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

    public void setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues = dimensionValues;
    }

    @JsonProperty
    public Map<String, Integer> getMetricValues()
    {
      return metricValues;
    }

    public void setMetricValues(Map<String, Integer> metricValues)
    {
      this.metricValues = metricValues;
    }
  }
}
