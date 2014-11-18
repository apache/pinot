package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;

import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
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
  @Path("/{collection}/{start}/{end}")
  @Timed
  public Result getTimeSeries(@PathParam("collection") String collection,
                              @PathParam("start") Long start,
                              @PathParam("end") Long end,
                              @Context UriInfo uriInfo)
  {
    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No collection " + collection);
    }

    StarTreeQuery query = createQueryBuilder(starTree, uriInfo)
            .setTimeRange(start, end)
            .build();

    List<StarTreeRecord> timeSeries = starTree.getTimeSeries(query);

    Result result = new Result();
    result.setTimeSeries(convertTimeSeries(timeSeries));
    result.setDimensionValues(query.getDimensionValues());
    return result;
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

  private static List<TimeSeriesElement> convertTimeSeries(List<StarTreeRecord> records)
  {
    List<TimeSeriesElement> timeSeries = new ArrayList<TimeSeriesElement>(records.size());

    for (StarTreeRecord record : records)
    {
      TimeSeriesElement element = new TimeSeriesElement();
      element.setTime(record.getTime());
      element.setMetricValues(record.getMetricValues());
      timeSeries.add(element);
    }

    return timeSeries;
  }

  public static class Result
  {
    @NotNull
    private Map<String, String> dimensionValues;

    @NotNull
    private List<TimeSeriesElement> timeSeries;

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
    public List<TimeSeriesElement> getTimeSeries()
    {
      return timeSeries;
    }

    @JsonProperty
    public void setTimeSeries(List<TimeSeriesElement> timeSeries)
    {
      this.timeSeries = timeSeries;
    }
  }

  public static class TimeSeriesElement
  {
    @NotNull
    private Long time;

    @NotNull
    private Map<String, Integer> metricValues;

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
}
