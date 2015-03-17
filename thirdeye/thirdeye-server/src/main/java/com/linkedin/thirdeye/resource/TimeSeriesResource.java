package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeStats;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.timeseries.FlotTimeSeries;
import com.linkedin.thirdeye.util.NormalizationMode;
import com.linkedin.thirdeye.util.QueryUtils;
import com.linkedin.thirdeye.views.TimeSeriesComponentView;
import com.sun.jersey.api.NotFoundException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Path("/timeSeries")
@Produces(MediaType.TEXT_HTML)
public class TimeSeriesResource
{
  private final StarTreeManager starTreeManager;

  public TimeSeriesResource(StarTreeManager starTreeManager)
  {
    this.starTreeManager = starTreeManager;
  }

  @GET
  @Path("/{collection}/{metrics}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}{normalized:(/normalized/[^/]+?)?}")
  @Timed
  public TimeSeriesComponentView getTimeSeriesComponentView(
          @PathParam("collection") String collection,
          @PathParam("metrics") String metrics,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          @PathParam("normalized") String normalized,
          @Context UriInfo uriInfo) throws Exception
  {
    return new TimeSeriesComponentView(getTimeSeriesComponentViewJson(
            collection, metrics, startMillis, endMillis, aggregate, movingAverage, normalized, uriInfo));
  }

  @GET
  @Path("/{collection}/{metrics}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}{normalized:(/normalized/[^/]+?)?}")
  @Timed
  @Produces(MediaType.APPLICATION_JSON)
  public List<FlotTimeSeries> getTimeSeriesComponentViewJson(
          @PathParam("collection") String collection,
          @PathParam("metrics") String metrics,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          @PathParam("normalized") String normalized,
          @Context UriInfo uriInfo) throws Exception
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

    // Should use aggregate?
    Long aggregateValue = "".equals(aggregate)
            ? null
            : bucketUnit.convert(Long.valueOf(aggregate.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    // Should use moving average?
    Long movingAverageValue = "".equals(movingAverage)
            ? null
            : bucketUnit.convert(Long.valueOf(movingAverage.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    // Should use normalization?
    NormalizationMode normalizationMode = null;
    String normalizationMetric = null;
    if ("".equals(normalized))
    {
      normalizationMode = NormalizationMode.NONE;
    }
    else
    {
      String[] tokens = normalized.split("/");
      if ("*".equals(tokens[2]))
      {
        normalizationMode = NormalizationMode.SELF;
      }
      else
      {
        normalizationMode = NormalizationMode.BASE;
        normalizationMetric = tokens[2];
      }
    }

    // Get collection times
    long start = bucketUnit.convert(startMillis, TimeUnit.MILLISECONDS) / bucketSize;
    long end = bucketUnit.convert(endMillis, TimeUnit.MILLISECONDS) / bucketSize;

    // Align start / end to aggregate value
    if (aggregateValue != null)
    {
      start = (start / aggregateValue) * aggregateValue;
      end = (end / aggregateValue) * aggregateValue;
    }

    long adjustedStartMillis = TimeUnit.MILLISECONDS.convert(start * bucketSize, bucketUnit);
    long adjustedEndMillis = TimeUnit.MILLISECONDS.convert(end * bucketSize, bucketUnit);

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

    // Compose result
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
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

      // Normalize
      switch (normalizationMode)
      {
        case SELF:
          timeSeries = MetricTimeSeriesUtils.normalize(timeSeries);
          break;
        case BASE:
          timeSeries = MetricTimeSeriesUtils.normalize(timeSeries, normalizationMetric);
          break;
        case NONE:
        default:
          // nothing
      }

      // Convert to milliseconds
      timeSeries = MetricTimeSeriesUtils.convertTimeToMillis(timeSeries, bucketSize, bucketUnit);

      result.put(entry.getKey(), timeSeries);
    }

    Set<String> allMetrics = new HashSet<String>();
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      allMetrics.add(metricSpec.getName());
    }

    // Get metric names
    List<String> metricNames;
    if ("*".equals(metrics))
    {
      metricNames = new ArrayList<String>(allMetrics);
      Collections.sort(metricNames);
    }
    else
    {
      metricNames = Arrays.asList(metrics.split(","));
      for (String metricName : metricNames)
      {
        if (!allMetrics.contains(metricName))
        {
          throw new NotFoundException("Unknown metric " + metricName + ", valid metrics are " + allMetrics);
        }
      }
    }

    // Convert
    List<FlotTimeSeries> flotSeries = new ArrayList<FlotTimeSeries>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : result.entrySet())
    {
      List<Long> times = new ArrayList<Long>(entry.getValue().getTimeWindowSet());
      Collections.sort(times);

      for (String metricName : metricNames)
      {
        Number[][] data = new Number[times.size()][];

        for (int i = 0; i < times.size(); i++)
        {
          long time = times.get(i);
          data[i] = new Number[] { time, entry.getValue().get(time, metricName) };
        }

        // Get ratio
        long startTime = Collections.min(entry.getValue().getTimeWindowSet());
        long endTime = Collections.max(entry.getValue().getTimeWindowSet());
        Number startValue = entry.getValue().get(startTime, metricName);
        Number endValue = entry.getValue().get(endTime, metricName);
        Double ratio = Double.POSITIVE_INFINITY;
        MetricType metricType = entry.getValue().getSchema().getMetricType(metricName);
        if (!NumberUtils.isZero(startValue, metricType))
        {
          ratio = 100 * (endValue.doubleValue() - startValue.doubleValue()) / startValue.doubleValue();
        }

        flotSeries.add(new FlotTimeSeries(
                metricName,
                String.format("(%.2f)%% %s", ratio, metricName),
                QueryUtils.convertDimensionKey(starTree.getConfig().getDimensions(), entry.getKey()),
                data,
                adjustedStartMillis,
                adjustedEndMillis));
      }
    }

    return flotSeries;
  }
}
