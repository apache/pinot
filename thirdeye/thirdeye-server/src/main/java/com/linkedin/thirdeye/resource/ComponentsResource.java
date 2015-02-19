package com.linkedin.thirdeye.resource;

import com.codahale.metrics.annotation.Timed;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.heatmap.ContributionDifferenceHeatMap;
import com.linkedin.thirdeye.heatmap.HeatMap;
import com.linkedin.thirdeye.heatmap.HeatMapCell;
import com.linkedin.thirdeye.heatmap.SelfRatioHeatMap;
import com.linkedin.thirdeye.heatmap.SnapshotHeatMap;
import com.linkedin.thirdeye.heatmap.VolumeHeatMap;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;
import com.linkedin.thirdeye.util.QueryUtils;
import com.linkedin.thirdeye.views.DefaultDashboardView;
import com.linkedin.thirdeye.views.DefaultLandingView;
import com.linkedin.thirdeye.views.HeatMapComponentView;
import com.linkedin.thirdeye.views.TimeSeriesComponentView;
import com.sun.jersey.api.NotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Path("/components")
@Produces(MediaType.TEXT_HTML)
public class ComponentsResource
{
  private enum NormalizationMode
  {
    NONE,
    SELF,
    BASE
  }

  private final StarTreeManager starTreeManager;
  private final ExecutorService parallelQueryExecutor;

  public ComponentsResource(StarTreeManager starTreeManager,
                            ExecutorService parallelQueryExecutor)
  {
    this.starTreeManager = starTreeManager;
    this.parallelQueryExecutor = parallelQueryExecutor;
  }

  @GET
  @Path("/dashboard/{collection}")
  @Timed
  public DefaultLandingView getDefaultLandingView(@PathParam("collection") String collection)
  {
    StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    return new DefaultLandingView(starTree.getConfig());
  }

  @GET
  @Path("/dashboard/{collection}/{heatMapType}/{metric}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}{normalized:(/normalized/[^/]+?)?}")
  @Timed
  public DefaultDashboardView getDefaultDashboardView(
          @PathParam("collection") String collection,
          @PathParam("heatMapType") String heatMapType,
          @PathParam("metric") String metric,
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

    TimeSeriesComponentView timeSeriesComponentView
            = getTimeSeriesComponentView(collection,
                                         "*",
                                         startMillis,
                                         endMillis,
                                         aggregate,
                                         movingAverage,
                                         normalized,
                                         uriInfo);

    HeatMapComponentView heatMapComponentView
            = getHeatMapComponentView(heatMapType,
                                      collection,
                                      metric,
                                      startMillis,
                                      endMillis,
                                      aggregate,
                                      movingAverage,
//                                      normalized,
                                      uriInfo);

    List<List<String>> disabledDimensions = new ArrayList<List<String>>();
    List<String> activeDimension = null;
    String queryString = uriInfo.getRequestUri().getQuery();
    if (queryString != null && queryString.length() > 0)
    {
      String[] queryComponents = queryString.split("&");

      for (int i = 0; i < queryComponents.length - 1; i++)
      {
        disabledDimensions.add(Arrays.asList(queryComponents[i].split("=")));
      }

      if (queryComponents.length != 0)
      {
        activeDimension = Arrays.asList(queryComponents[queryComponents.length - 1].split("="));
      }
    }

    return new DefaultDashboardView(starTree.getConfig(),
                                    metric,
                                    endMillis,
                                    disabledDimensions,
                                    activeDimension,
                                    timeSeriesComponentView,
                                    heatMapComponentView);
  }

  @GET
  @Path("/timeSeries/{collection}/{metrics}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}{normalized:(/normalized/[^/]+?)?}")
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
        timeSeries = MetricTimeSeriesUtils.aggregate(timeSeries, aggregateValue, end);
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

    List<String> metricNames;
    if ("*".equals(metrics))
    {
      metricNames = null;
    }
    else
    {
      metricNames = Arrays.asList(metrics.split(","));
    }

    return new TimeSeriesComponentView(starTree.getConfig(),
                                       metricNames,
                                       result,
                                       TimeUnit.MILLISECONDS.convert(start, bucketUnit),
                                       TimeUnit.MILLISECONDS.convert(end, bucketUnit));
  }

  @GET
  @Path("/heatMap/{type}/{collection}/{metric}/{startMillis}/{endMillis}{aggregate:(/aggregate/[^/]+?)?}{movingAverage:(/movingAverage/[^/]+?)?}")
  @Timed
  public HeatMapComponentView getHeatMapComponentView(
          @PathParam("type") String type,
          @PathParam("collection") String collection,
          @PathParam("metric") String metric,
          @PathParam("startMillis") Long startMillis,
          @PathParam("endMillis") Long endMillis,
          @PathParam("aggregate") String aggregate,
          @PathParam("movingAverage") String movingAverage,
          final @Context UriInfo uriInfo) throws Exception
  {
    final StarTree starTree = starTreeManager.getStarTree(collection);
    if (starTree == null)
    {
      throw new NotFoundException("No collection " + collection);
    }

    int bucketSize
            = starTree.getConfig().getTime().getBucket().getSize();
    TimeUnit bucketUnit
            = starTree.getConfig().getTime().getBucket().getUnit();

    Long aggregateValue = "".equals(aggregate)
            ? null
            : bucketUnit.convert(Long.valueOf(aggregate.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    Long movingAverageValue = "".equals(movingAverage)
            ? null
            : bucketUnit.convert(Long.valueOf(movingAverage.split("/")[2]), TimeUnit.MILLISECONDS) / bucketSize;

    long baselineStart = bucketUnit.convert(startMillis, TimeUnit.MILLISECONDS) / bucketSize;
    long baselineEnd = baselineStart + (aggregateValue == null ? 0 : aggregateValue);
    long currentStart = bucketUnit.convert(endMillis, TimeUnit.MILLISECONDS) / bucketSize;
    long currentEnd = currentStart + (aggregateValue == null ? 0 : aggregateValue);

    if (aggregateValue != null)
    {
      baselineStart = (baselineStart / aggregateValue) * aggregateValue;
      baselineEnd = (baselineEnd / aggregateValue) * aggregateValue;
      currentStart = (currentStart / aggregateValue) * aggregateValue;
      currentEnd = (currentEnd / aggregateValue) * aggregateValue;
    }

    final TimeRange timeRange;
    if (movingAverageValue == null && aggregateValue == null)
    {
      timeRange = new TimeRange(baselineStart, currentEnd);
    }
    else if (movingAverageValue != null && aggregateValue == null)
    {
      timeRange = new TimeRange(baselineStart - movingAverageValue, currentEnd);
    }
    else if (movingAverageValue == null && aggregateValue != null)
    {
      timeRange = new TimeRange(baselineStart, currentEnd + aggregateValue);
    }
    else
    {
      timeRange = new TimeRange(baselineStart - (movingAverageValue / aggregateValue) * aggregateValue, currentEnd + aggregateValue);
    }

    Map<String, Map<String, MetricTimeSeries>> data
            = new HashMap<String, Map<String, MetricTimeSeries>>();

    for (DimensionSpec dimension : starTree.getConfig().getDimensions())
    {
      Map<String, MetricTimeSeries> timeSeriesByDimensionValue
              = QueryUtils.groupByQuery(parallelQueryExecutor, starTree, dimension.getName(), timeRange, uriInfo);

      for (Map.Entry<String, MetricTimeSeries> entry : timeSeriesByDimensionValue.entrySet())
      {
        MetricTimeSeries timeSeries = entry.getValue();

        // Aggregate
        if (aggregateValue != null)
        {
          timeSeries = MetricTimeSeriesUtils.aggregate(timeSeries, aggregateValue, currentEnd);
        }

        // Take moving average
        if (movingAverageValue != null)
        {
          timeSeries = MetricTimeSeriesUtils.getSimpleMovingAverage(
                  timeSeries, baselineStart, currentEnd, movingAverageValue);
        }

        timeSeriesByDimensionValue.put(entry.getKey(), timeSeries);
      }

      if (timeSeriesByDimensionValue.size() > 1) // i.e. the dimension value was not fixed
      {
        data.put(dimension.getName(), timeSeriesByDimensionValue);
      }
    }

    // Get metric type
    MetricType metricType = null;
    for (MetricSpec metricSpec : starTree.getConfig().getMetrics())
    {
      if (metricSpec.getName().equals(metric))
      {
        metricType = metricSpec.getType();
        break;
      }
    }

    // Pick heat map
    HeatMap heatMap;
    if ("volume".equals(type))
    {
      heatMap = new VolumeHeatMap();
    }
    else if ("selfRatio".equals(type))
    {
      heatMap = new SelfRatioHeatMap(metricType);
    }
    else if ("contributionDifference".equals(type))
    {
      heatMap = new ContributionDifferenceHeatMap(metricType);
    }
    else if ("snapshot".equals(type))
    {
      heatMap = new SnapshotHeatMap(2);
    }
    else
    {
      throw new NotFoundException("No heat map type " + type);
    }

    Map<String, List<HeatMapCell>> heatMaps = new HashMap<String, List<HeatMapCell>>();

    for (Map.Entry<String, Map<String, MetricTimeSeries>> entry : data.entrySet())
    {
      heatMaps.put(entry.getKey(), heatMap.generateHeatMap(
              metric,
              entry.getValue(),
              new TimeRange(baselineStart, baselineEnd),
              new TimeRange(currentStart, currentEnd)));
    }

    return new HeatMapComponentView(heatMaps);
  }
}
