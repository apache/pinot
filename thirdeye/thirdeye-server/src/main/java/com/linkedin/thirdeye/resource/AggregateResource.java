package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.util.QueryUtils;
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
import java.util.concurrent.TimeUnit;

@Path("/aggregate")
public class AggregateResource
{
  private final StarTreeManager starTreeManager;

  public AggregateResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}/{startMillis}/{endMillis}")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public List<AggregateResult> getAggregate(
          @PathParam("collection") String collection,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @Context UriInfo uriInfo)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    int bucketSize
            = starTree.getConfig().getTime().getBucket().getSize();
    TimeUnit bucketUnit
            = starTree.getConfig().getTime().getBucket().getUnit();

    // Get collection times
    long start = bucketUnit.convert(startMillis, TimeUnit.MILLISECONDS) / bucketSize;
    long end = bucketUnit.convert(endMillis, TimeUnit.MILLISECONDS) / bucketSize;

    long queryStartTime = System.currentTimeMillis();
    Map<DimensionKey, MetricTimeSeries> queryResult = QueryUtils.doQuery(starTree, start, end, uriInfo);
    long queryTimeMillis = System.currentTimeMillis() - queryStartTime;

    List<AggregateResult> clientResult = new ArrayList<AggregateResult>(queryResult.size());

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : queryResult.entrySet())
    {
      Map<String, String> dimensionValues
              = QueryUtils.convertDimensionKey(starTree.getConfig().getDimensions(), entry.getKey());

      Number[] metricSums = entry.getValue().getMetricSums();

      Map<String, Number> metricValues = new HashMap<String, Number>(metricSums.length);

      for (int i = 0; i < starTree.getConfig().getMetrics().size(); i++)
      {
        MetricSpec metricSpec = starTree.getConfig().getMetrics().get(i);
        metricValues.put(metricSpec.getName(), metricSums[i]);
      }

      clientResult.add(new AggregateResult(dimensionValues, metricValues, queryTimeMillis));
    }

    return clientResult;
  }

  public static class AggregateResult
  {
    private final Map<String, String> dimensionValues;
    private final Map<String, Number> metricValues;
    private final long queryTimeMillis;

    public AggregateResult(Map<String, String> dimensionValues,
                           Map<String, Number> metricValues,
                           long queryTimeMillis)
    {
      this.dimensionValues = dimensionValues;
      this.metricValues = metricValues;
      this.queryTimeMillis = queryTimeMillis;
    }

    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    public Map<String, Number> getMetricValues()
    {
      return metricValues;
    }

    public long getQueryTimeMillis()
    {
      return queryTimeMillis;
    }
  }
}
