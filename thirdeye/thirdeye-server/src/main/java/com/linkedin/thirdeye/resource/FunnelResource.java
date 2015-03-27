package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.funnel.Funnel;
import com.linkedin.thirdeye.funnel.FunnelRow;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.util.QueryUtils;
import com.linkedin.thirdeye.views.FunnelComponentView;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Path("/funnel")
@Produces(MediaType.TEXT_HTML)
public class FunnelResource
{
  private final StarTreeManager starTreeManager;

  public FunnelResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{type}/{collection}/{metrics}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}")
  @Timed
  public FunnelComponentView getFunnelView(
          @PathParam("type") String type,
          @PathParam("collection") String collection,
          @PathParam("metrics") String metrics,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          @Context UriInfo uriInfo) throws Exception
  {
    return new FunnelComponentView(getFunnelViewJson(type, collection, metrics, startMillis, endMillis, aggregate, movingAverage, uriInfo));
  }

  @GET
  @Path("/{type}/{collection}/{metrics}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public List<Funnel> getFunnelViewJson(
          @PathParam("type") String type,
          @PathParam("collection") String collection,
          @PathParam("metrics") String metrics,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          @Context UriInfo uriInfo) throws Exception
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    Funnel.Type funnelType;
    try
    {
      funnelType = Funnel.Type.valueOf(type.toUpperCase());
    }
    catch (Exception e)
    {
      throw new NotFoundException("Unrecognized funnel type " + type);
    }

    List<String> funnelMetrics = Arrays.asList(metrics.split(","));

    // Check funnel
    if (funnelMetrics.size() < 2)
    {
      throw new WebApplicationException(new IllegalStateException(
              "Funnel must specify at least two metrics: " + funnelMetrics), Response.Status.BAD_REQUEST);
    }

    // Get top metric spec
    MetricSpec topMetric = null;
    Map<String, MetricSpec> metricSpecs = new HashMap<String, MetricSpec>();
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      if (metricSpec.getName().equals(funnelMetrics.get(0)))
      {
        topMetric = metricSpec;
      }
      metricSpecs.put(metricSpec.getName(), metricSpec);
    }
    if (topMetric == null)
    {
      throw new WebApplicationException(new IllegalStateException(
              "Baseline metric not found " + funnelMetrics.get(0)), 500);
    }

    int bucketSize
            = starTree.getConfig().getTime().getBucket().getSize();
    TimeUnit bucketUnit
            = starTree.getConfig().getTime().getBucket().getUnit();

    // Should use aggregate?
    Long aggregateValue = "".equals(aggregate)
            ? null
            : bucketUnit.convert(Long.valueOf(aggregate.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    // Should use moving average?
    Long movingAverageValue = "".equals(movingAverage)
            ? null
            : bucketUnit.convert(Long.valueOf(movingAverage.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    // Get collection times
    long start = bucketUnit.convert(startMillis, TimeUnit.MILLISECONDS) / bucketSize;
    long end = bucketUnit.convert(endMillis, TimeUnit.MILLISECONDS) / bucketSize;

    // Align start / end to aggregate value
    if (aggregateValue != null)
    {
      start = (start / aggregateValue) * aggregateValue;
      end = (end / aggregateValue) * aggregateValue;
    }

    // Check time
    StarTreeStats stats = starTree.getStats();
    if (!new TimeRange(stats.getMinTime(), stats.getMaxTime()).contains(new TimeRange(start, end)))
    {
      throw new NotFoundException(
              "Query (" + QueryUtils.getDateTime(start, bucketSize, bucketUnit) + ", "
                      + QueryUtils.getDateTime(end, bucketSize, bucketUnit)
                      + ") not in range ("
                      + QueryUtils.getDateTime(stats.getMinTime(), bucketSize, bucketUnit)
                      + ", " + QueryUtils.getDateTime(stats.getMaxTime(), bucketSize, bucketUnit) + ")");
    }

  //Check dimensions
    String invalidDimension = QueryUtils.checkDimensions(starTree, uriInfo);
    if (invalidDimension != null)
    {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).
          header("No dimension ", invalidDimension).entity("No dimension : "+ invalidDimension).build());
    }

    // Do query
    Map<DimensionKey, MetricTimeSeries> result;
    if (movingAverageValue == null && aggregateValue == null)
    {
      result = QueryUtils.doQuery(starTree, start, end, uriInfo);
    }
    else if (movingAverageValue != null && aggregateValue == null)
    {
      result = QueryUtils.doQuery(starTree, start - movingAverageValue, end, uriInfo);
    }
    else if (movingAverageValue == null && aggregateValue != null)
    {
      result = QueryUtils.doQuery(starTree, start, end + aggregateValue, uriInfo);
    }
    else
    {
      result = QueryUtils.doQuery(starTree, start - (movingAverageValue / aggregateValue) * aggregateValue, end + aggregateValue, uriInfo);
    }

    // Compose funnels
    List<Funnel> funnels = new ArrayList<Funnel>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      List<FunnelRow> rows = new ArrayList<FunnelRow>();

      // Apply aggregate and/or moving average
      MetricTimeSeries timeSeries = entry.getValue();

      // Aggregate
      if (aggregateValue != null)
      {
        timeSeries = MetricTimeSeriesUtils.aggregate(timeSeries, aggregateValue, end + aggregateValue);
      }

      // Take moving average
      if (movingAverageValue != null)
      {
        timeSeries = MetricTimeSeriesUtils.getSimpleMovingAverage(timeSeries, start, end, movingAverageValue);
      }

      // Top
      rows.add(FunnelRow.createTopRow(topMetric));
      double topStartValue = timeSeries.get(start, topMetric.getName()).doubleValue();
      double topEndValue = timeSeries.get(end, topMetric.getName()).doubleValue();

      // Subsequent
      for (int i = 1; i < funnelMetrics.size(); i++)
      {
        // Previous
        MetricSpec previousMetric = metricSpecs.get(funnelMetrics.get(i - 1));
        if (previousMetric == null)
        {
          throw new WebApplicationException(new IllegalStateException(
                  "Metric not found " + funnelMetrics.get(i - 1)), 500);
        }
        double previousStartValue = timeSeries.get(start, previousMetric.getName()).doubleValue();
        double previousEndValue = timeSeries.get(end, previousMetric.getName()).doubleValue();

        // Current
        MetricSpec currentMetric = metricSpecs.get(funnelMetrics.get(i));
        if (currentMetric == null)
        {
          throw new WebApplicationException(new IllegalStateException(
                  "Metric not found " + funnelMetrics.get(i)), 500);
        }
        double currentStartValue = timeSeries.get(start, currentMetric.getName()).doubleValue();
        double currentEndValue = timeSeries.get(end, currentMetric.getName()).doubleValue();

        switch (funnelType)
        {
          case TOP:
            double currentToTopStart = 0;
            double currentToTopEnd = 0;
            if (topStartValue > 0)
            {
              currentToTopStart = currentStartValue / topStartValue;
            }
            if (topEndValue > 0)
            {
              currentToTopEnd = currentEndValue / topEndValue;
            }
            rows.add(new FunnelRow(currentMetric, currentToTopStart, currentToTopEnd));
            break;
          case PREVIOUS:
            double currentToPreviousStart = 0;
            double currentToPreviousEnd = 0;
            if (previousStartValue > 0)
            {
              currentToPreviousStart = currentStartValue / previousStartValue;
            }
            if (previousEndValue > 0)
            {
              currentToPreviousEnd = currentEndValue / previousEndValue;
            }
            rows.add(new FunnelRow(currentMetric, currentToPreviousStart, currentToPreviousEnd));
            break;
          default:
            throw new IllegalArgumentException("Unsupported funnel type " + type);
        }
      }

      funnels.add(new Funnel(QueryUtils.convertDimensionKey(starTree.getConfig().getDimensions(), entry.getKey()), rows));
    }

    return funnels;
  }
}
