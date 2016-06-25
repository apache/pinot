package com.linkedin.thirdeye.detector.resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.MetricsGraphicsTimeSeries;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/time-series/metrics-graphics")
public class MetricsGraphicsTimeSeriesResource {
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private final TimeOnTimeComparisonHandler timeOnTimeComparisonHandler;
  private final AnomalyResultDAO resultDAO;
  private final QueryCache queryCache;

  public MetricsGraphicsTimeSeriesResource(AnomalyResultDAO resultDAO) {
    this.queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    this.timeOnTimeComparisonHandler = new TimeOnTimeComparisonHandler(queryCache);
    this.resultDAO = resultDAO;
  }

  // TODO this method is temporarily commented out to allow for other progress to be pushed.
  // @GET
  // @Path("/{collection}/{metric}/{startTimeISO}/{endTimeISO}")
  // @UnitOfWork
  // @Produces(MediaType.APPLICATION_JSON)
  // public MetricsGraphicsTimeSeries getTimeSeries(@PathParam("collection") String collection,
  // @PathParam("metric") String metric, @PathParam("startTimeISO") String startTimeISO,
  // @PathParam("endTimeISO") String endTimeISO, @QueryParam("timeZone") String timeZone,
  // @QueryParam("bucketSize") String bucketSize, @QueryParam("groupBy") String groupBy,
  // @QueryParam("topK") @DefaultValue("5") Integer topK,
  // @QueryParam("functionId") Long functionId, @QueryParam("anomalyIds") List<Long> anomalyIds,
  // @QueryParam("overlayUnit") String overlayUnitStr,
  // @QueryParam("overlaySize") String overlaySizeStr, @Context UriInfo uriInfo) throws Exception {
  // DateTime startTime = parseDateTime(startTimeISO, timeZone);
  // DateTime endTime = parseDateTime(endTimeISO, timeZone);
  // Multimap<String, String> fixedValues = extractDimensionValues(uriInfo);
  //
  // MetricFunction metricFunction = new MetricFunction(Function.SUM, metric);
  // TimeGranularity aggTimeGranularity = getAggTimeGranularity(bucketSize, collection);
  // // Get time series data
  // // ThirdEyeMetricFunction metricFunction = getMetricFunction(metric, bucketSize, collection);
  //
  // // TODO
  // // TimeOnTimeComparisonRequest request = new TimeOnTimeComparisonRequest();
  // // request.setCollectionName(collection);
  // // request.setMetricFunctions(Collections.singletonList(metricFunction));
  // // request.setCurrentStart(startTime);
  // // request.setCurrentEnd(currentEnd);
  // // request.setAggregationTimeGranularity(aggTimeGranularity);
  // ThirdEyeRequestBuilder req =
  // new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
  // .setStartTime(startTime).setEndTime(endTime).setDimensionValues(fixedValues);
  //
  // if (groupBy != null) {
  // req.setGroupBy(groupBy);
  // }
  //
  // // Do query
  // Map<DimensionKey, MetricTimeSeries> res = thirdEyeClient.execute(req.build());
  // Map<DimensionKey, List<Map<String, Object>>> dataByDimensionKey = new HashMap<>(res.size());
  // final Map<DimensionKey, Double> totalVolume = new HashMap<>(res.size());
  //
  // // Transform result
  // for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
  // List<Map<String, Object>> series =
  // convertSeries(entry.getKey(), entry.getValue(), metric, totalVolume);
  // if (!series.isEmpty()) {
  // dataByDimensionKey.put(entry.getKey(), series);
  // }
  // }
  //
  // // Sort and filter top K
  // List<DimensionKey> sortedAndFilteredKeys = new ArrayList<>(dataByDimensionKey.keySet());
  // Collections.sort(sortedAndFilteredKeys, new Comparator<DimensionKey>() {
  // @Override
  // public int compare(DimensionKey o1, DimensionKey o2) {
  // return (int) (totalVolume.get(o2) - totalVolume.get(o1)); // descending
  // }
  // });
  // if (topK < sortedAndFilteredKeys.size()) {
  // sortedAndFilteredKeys = sortedAndFilteredKeys.subList(0, topK);
  // }
  //
  // // Compose data
  // List<String> legend = new ArrayList<>();
  // List<List<Map<String, Object>>> data = new ArrayList<>();
  // for (DimensionKey key : sortedAndFilteredKeys) {
  // data.add(dataByDimensionKey.get(key));
  //
  // // Legend
  // List<String> dimensionValues = new ArrayList<>();
  // for (String value : key.getDimensionValues()) {
  // if ("*".equals(value)) {
  // dimensionValues.add("ALL");
  // } else {
  // dimensionValues.add(String.format("\"%s\"", value));
  // }
  // }
  // legend.add(CSV.join(dimensionValues));
  // }
  //
  // // Add overlay
  // if (overlayUnitStr != null && overlaySizeStr != null) {
  // if (groupBy != null) {
  // throw new BadRequestException("Cannot specify both overlay and groupBy");
  // }
  //
  // // Parse overlay
  // TimeUnit overlayUnit = TimeUnit.valueOf(overlayUnitStr.toUpperCase());
  // int overlaySize = Integer.valueOf(overlaySizeStr);
  // long overlayMillis = overlayUnit.toMillis(overlaySize);
  //
  // // Get overlay
  // DateTime overlayStart = startTime.minus(overlayMillis);
  // DateTime overlayEnd = endTime.minus(overlayMillis);
  //
  // long shiftMillis = endTime.getMillis() - overlayEnd.getMillis();
  // ThirdEyeRequest overlayReq = new ThirdEyeRequestBuilder().setCollection(collection)
  // .setMetricFunction(metricFunction).setStartTime(overlayStart).setEndTime(overlayEnd)
  // .setDimensionValues(fixedValues).build();
  //
  // Map<DimensionKey, MetricTimeSeries> overlayRes = thirdEyeClient.execute(overlayReq);
  // for (Map.Entry<DimensionKey, MetricTimeSeries> entry : overlayRes.entrySet()) {
  // List<Map<String, Object>> overlaySeries =
  // convertSeries(entry.getKey(), entry.getValue(), metric, null);
  // overlaySeries = shiftSeries(overlaySeries, shiftMillis); // TODO figure out why the result
  // // of the overlay request still
  // // needs to be shifted?
  // // WHY IS THIS HAPPENING?
  // data.add(overlaySeries);
  // }
  // }
  //
  // MetricsGraphicsTimeSeries timeSeries = new MetricsGraphicsTimeSeries();
  // timeSeries.setTitle(metric + (groupBy == null || "".equals(groupBy) ? "" : " by " + groupBy));
  // timeSeries.setDescription(req.toString());
  // timeSeries.setData(data);
  // timeSeries.setxAccessor("time");
  // timeSeries.setyAccessor("value");
  //
  // // Get any anomalies
  // List<AnomalyResult> anomalies;
  // boolean isAnomalyIdsSpecified = (anomalyIds != null && !anomalyIds.isEmpty());
  // if (functionId != null && isAnomalyIdsSpecified) {
  // throw new BadRequestException("Cannot specify both functionId and anomalyIds");
  // } else if (functionId != null) {
  // anomalies = resultDAO.findAllByCollectionTimeFunctionIdAndMetric(collection, metric,
  // functionId, startTime, endTime);
  // } else if (isAnomalyIdsSpecified) {
  // anomalies = new ArrayList<>(anomalyIds.size());
  // for (Long anomalyId : anomalyIds) {
  // AnomalyResult anomaly = resultDAO.findById(anomalyId);
  // if (anomaly != null) {
  // anomalies.add(anomaly);
  // }
  // }
  // } else {
  // anomalies =
  // resultDAO.findAllByCollectionTimeAndMetric(collection, metric, startTime, endTime);
  // }
  // CollectionSchema schema = thirdEyeClient.getCollectionSchema(collection);
  // List<String> dimensionNames = schema.getDimensionNames();
  // if (groupBy == null) {
  // // Only show values that match current combination
  // List<AnomalyResult> filtered = new ArrayList<>();
  // for (AnomalyResult anomaly : anomalies) {
  // String[] dimensions = anomaly.getDimensions().split(",", dimensionNames.size());
  // // TODO what to do if the dimension key has changed?
  // boolean matches = true;
  //
  // for (int i = 0; i < dimensionNames.size(); i++) {
  // String dimensionName = dimensionNames.get(i);
  // if (!fixedValues.containsKey(dimensionName) && !"*".equals(dimensions[i])) {
  // matches = false;
  // break;
  // } else if (fixedValues.containsKey(dimensionName)
  // && !fixedValues.get(dimensionName).contains(dimensions[i])) {
  // matches = false;
  // break;
  // }
  // }
  //
  // if (matches) {
  // filtered.add(anomaly);
  // }
  // }
  // anomalies = filtered;
  // } else {
  // // If there's a group by, only show those for the visible dimension values
  // int idx = -1;
  // for (int i = 0; i < dimensionNames.size(); i++) {
  // if (dimensionNames.get(i).equals(groupBy)) {
  // idx = i;
  // break;
  // }
  // }
  //
  // // Get the represented values from the time series
  // Set<String> representedValues = new HashSet<>();
  // for (DimensionKey key : sortedAndFilteredKeys) {
  // representedValues.add(key.getDimensionValues()[idx]);
  // }
  //
  // // Filter the anomalies
  // List<AnomalyResult> filtered = new ArrayList<>();
  // for (AnomalyResult anomaly : anomalies) {
  // String[] dimensions = anomaly.getDimensions().split(",", dimensionNames.size());
  // if (representedValues.contains(dimensions[idx])) {
  // filtered.add(anomaly);
  // }
  // }
  // anomalies = filtered;
  // }
  //
  // // Add the anomaly markers
  // List<Map<String, Object>> markers = new ArrayList<>();
  // for (AnomalyResult anomaly : anomalies) {
  // markers.addAll(createAnomalyMarkers(anomaly, startTime, endTime));
  // }
  // timeSeries.setMarkers(markers);
  // timeSeries.setAnomalyResults(anomalies);
  //
  // if (legend.size() > 1) {
  // timeSeries.setLegend(legend);
  // }
  //
  // return timeSeries;
  // }

  @GET
  @Path("/dummy")
  @UnitOfWork
  @Produces(MediaType.APPLICATION_JSON)
  public MetricsGraphicsTimeSeries getDummyTimeSeries() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();

    List<Map<String, Object>> data =
        objectMapper.readValue(ClassLoader.getSystemResourceAsStream("dummy-time-series.json"),
            new TypeReference<List<Map<String, Object>>>() {
            });

    Map<String, Object> marker = new HashMap<>();
    marker.put("year", 1964);
    marker.put("label", "The creeping terror released");
    List<Map<String, Object>> markers = ImmutableList.of(marker);

    MetricsGraphicsTimeSeries timeSeries = new MetricsGraphicsTimeSeries();
    timeSeries.setTitle("UFO Sightings");
    timeSeries.setDescription("Yearly UFO sightings from the year 1945 to 2010.");
    timeSeries.setData(data);
    timeSeries.setMarkers(markers);
    timeSeries.setxAccessor("year");
    timeSeries.setyAccessor("sightings");

    return timeSeries;
  }

  private static DateTime parseDateTime(String dateTimeString, String timeZone) {
    DateTime dateTime =
        ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime(dateTimeString);
    if (timeZone != null) {
      dateTime = dateTime.toDateTime(DateTimeZone.forID(timeZone));
    }
    return dateTime;
  }

  // TODO delete me
  // private ThirdEyeMetricFunction getMetricFunction(String metric, String bucketSize,
  // String collection) throws Exception {
  // if (bucketSize == null) {
  // // default to data bucket size
  // CollectionSchema config = thirdEyeClient.getCollectionSchema(collection);
  // TimeGranularity bucket = config.getTime().getBucket();
  // return new ThirdEyeMetricFunction(bucket, Collections.singletonList(metric));
  // }
  // // return new MetricFunction(Function.SUM, metric);
  // return ThirdEyeMetricFunction.fromStr(String.format("AGGREGATE_%s(%s)", bucketSize, metric));
  // }

  /**
   * Returns time granularity parsed from bucketSize of format (size)_(unit), or retrieves the
   * default granularity from the input collection.
   * @throws Exception
   */
  private TimeGranularity getAggTimeGranularity(String bucketSize, String collection)
      throws Exception {
    if (bucketSize == null) {
      CollectionSchema collectionSchema =
          timeOnTimeComparisonHandler.getClient().getCollectionSchema(collection);
      return collectionSchema.getTime().getDataGranularity();
    } else {
      String[] split = bucketSize.split("_");
      String sizeStr = split[0];
      String unitStr = split[1].toUpperCase();
      return new TimeGranularity(Integer.parseInt(sizeStr), TimeUnit.valueOf(unitStr));
    }
  }

  private Multimap<String, String> extractDimensionValues(UriInfo uriInfo) {
    Multimap<String, String> values = LinkedListMultimap.create();
    for (String key : uriInfo.getQueryParameters().keySet()) {
      if (key.startsWith("dim_")) {
        String dimensionName = key.substring("dim_".length());
        values.putAll(dimensionName, uriInfo.getQueryParameters().get(key));
      }
    }
    return values;
  }

  private List<Map<String, Object>> shiftSeries(List<Map<String, Object>> series,
      long shiftMillis) {
    List<Map<String, Object>> shifted = new ArrayList<>();
    for (Map<String, Object> point : series) {
      shifted.add(ImmutableMap.of("time", (Long) point.get("time") + shiftMillis, "value",
          point.get("value")));
    }
    return shifted;
  }

  private List<Map<String, Object>> convertSeries(DimensionKey dimensionKey,
      MetricTimeSeries timeSeries, String metric, Map<DimensionKey, Double> totalVolume) {
    List<Long> sortedTimes = new ArrayList<>(timeSeries.getTimeWindowSet());
    Collections.sort(sortedTimes);

    Set<Long> timeSet = new HashSet<>();
    if (sortedTimes.size() > 1) {
      // Get min difference
      Long minDiff = null;
      for (int i = 0; i < sortedTimes.size() - 1; i++) {
        long diff = sortedTimes.get(i + 1) - sortedTimes.get(i);
        if (minDiff == null || diff < minDiff) {
          minDiff = diff;
        }
      }

      // Fill in any missing spots with zero
      for (long time = sortedTimes.get(0); time <= sortedTimes.get(sortedTimes.size() - 1); time +=
          minDiff) {
        timeSet.add(time);
      }
    } else {
      timeSet.addAll(sortedTimes);
    }

    // Series
    List<Map<String, Object>> series = new ArrayList<>(timeSeries.getTimeWindowSet().size());
    for (Long time : timeSet) {
      Number value = timeSeries.get(time, metric);
      if (value == null) {
        value = 0;
      }
      series.add(ImmutableMap.of("time", time, "value", (Object) value));

      // Total volume for top K
      if (totalVolume != null) {
        Double volume = totalVolume.get(dimensionKey);
        if (volume == null) {
          volume = 0.0;
        }
        totalVolume.put(dimensionKey, volume + value.doubleValue());
      }
    }

    return series;
  }

  // TODO provide a better abstraction on the concept of a data point
  private Map<String, Object> createGraphPoint(Long time, Number value) {
    return ImmutableMap.of("time", time, "value", (Object) value);
  }

  /**
   * Returns a list consisting of single point if the anomaly has no end time or a pair of points if
   * the anomaly is an
   * interval. If any point is outside the provided range, it is not added (potentially resulting in
   * an empty list)
   */
  private List<Map<String, Object>> createAnomalyMarkers(AnomalyResult anomaly, DateTime startTime,
      DateTime endTime) {
    List<Map<String, Object>> markers = new ArrayList<>(2);
    if (anomaly.getEndTimeUtc() == null) {
      // Point
      if (anomaly.getStartTimeUtc() >= startTime.getMillis()
          && anomaly.getStartTimeUtc() <= endTime.getMillis()) {
        markers.add(
            createAnomalyMarkerPoint(anomaly.getStartTimeUtc(), String.valueOf(anomaly.getId())));
      }
    } else {
      // Interval
      if (anomaly.getStartTimeUtc() >= startTime.getMillis()
          && anomaly.getStartTimeUtc() <= endTime.getMillis()) {
        markers
            .add(createAnomalyMarkerPoint(anomaly.getStartTimeUtc(), "START_" + anomaly.getId()));
      }
      if (anomaly.getEndTimeUtc() >= startTime.getMillis()
          && anomaly.getEndTimeUtc() <= endTime.getMillis()) {
        markers.add(createAnomalyMarkerPoint(anomaly.getEndTimeUtc(), "END_" + anomaly.getId()));
      }
    }
    return markers;
  }

  // TODO provide a better abstraction for anomaly markers
  private Map<String, Object> createAnomalyMarkerPoint(Long time, String label) {
    return ImmutableMap.of("time", time, "label", (Object) label);
  }

}
