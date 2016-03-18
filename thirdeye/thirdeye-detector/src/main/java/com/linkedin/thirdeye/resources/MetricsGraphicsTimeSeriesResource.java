package com.linkedin.thirdeye.resources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricsGraphicsTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeMetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.db.AnomalyResultDAO;

import io.dropwizard.hibernate.UnitOfWork;

@Path("/time-series/metrics-graphics")
public class MetricsGraphicsTimeSeriesResource {
  private static final Joiner CSV = Joiner.on(", ");
  private final ThirdEyeClient thirdEyeClient;
  private final AnomalyResultDAO resultDAO;

  public MetricsGraphicsTimeSeriesResource(ThirdEyeClient thirdEyeClient,
      AnomalyResultDAO resultDAO) {
    this.thirdEyeClient = thirdEyeClient;
    this.resultDAO = resultDAO;
  }

  @GET
  @Path("/{collection}/{metric}/{startTimeISO}/{endTimeISO}")
  @UnitOfWork
  @Produces(MediaType.APPLICATION_JSON)
  public MetricsGraphicsTimeSeries getTimeSeries(@PathParam("collection") String collection,
      @PathParam("metric") String metric, @PathParam("startTimeISO") String startTimeISO,
      @PathParam("endTimeISO") String endTimeISO, @QueryParam("timeZone") String timeZone,
      @QueryParam("bucketSize") String bucketSize, @QueryParam("groupBy") String groupBy,
      @QueryParam("topK") @DefaultValue("5") Integer topK,
      @QueryParam("functionId") Long functionId, @QueryParam("anomalyIds") List<Long> anomalyIds,
      @QueryParam("overlay") String overlay, @Context UriInfo uriInfo) throws Exception {
    DateTime startTime = parseDateTime(startTimeISO, timeZone);
    DateTime endTime = parseDateTime(endTimeISO, timeZone);
    Multimap<String, String> fixedValues = extractDimensionValues(uriInfo);

    // Get time series data
    ThirdEyeMetricFunction metricFunction = getMetricFunction(metric, bucketSize, collection);
    ThirdEyeRequestBuilder requestBuilder =
        new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
            .setStartTimeInclusive(startTime).setEndTime(endTime).setDimensionValues(fixedValues);

    if (groupBy != null) {
      requestBuilder.setGroupBy(groupBy);
    }

    ThirdEyeRequest request = requestBuilder.build();
    // Do query
    Map<DimensionKey, MetricTimeSeries> res = thirdEyeClient.execute(request);
    Map<DimensionKey, List<Map<String, Object>>> dataByDimensionKey = new HashMap<>(res.size());
    final Map<DimensionKey, Double> totalVolume = new HashMap<>(res.size());

    // Transform result
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      List<Map<String, Object>> series =
          convertSeries(entry.getKey(), entry.getValue(), metric, totalVolume);
      if (!series.isEmpty()) {
        dataByDimensionKey.put(entry.getKey(), series);
      }
    }

    // Sort and filter top K
    List<DimensionKey> sortedAndFilteredKeys = new ArrayList<>(dataByDimensionKey.keySet());
    Collections.sort(sortedAndFilteredKeys, new Comparator<DimensionKey>() {
      @Override
      public int compare(DimensionKey o1, DimensionKey o2) {
        return (int) (totalVolume.get(o2) - totalVolume.get(o1)); // descending
      }
    });
    if (topK < sortedAndFilteredKeys.size()) {
      sortedAndFilteredKeys = sortedAndFilteredKeys.subList(0, topK);
    }

    // Compose data
    List<String> legend = new ArrayList<>();
    List<List<Map<String, Object>>> data = new ArrayList<>();
    for (DimensionKey key : sortedAndFilteredKeys) {
      data.add(dataByDimensionKey.get(key));

      // Legend
      List<String> dimensionValues = new ArrayList<>();
      for (String value : key.getDimensionValues()) {
        if ("*".equals(value)) {
          dimensionValues.add("ALL");
        } else {
          dimensionValues.add(String.format("\"%s\"", value));
        }
      }
      legend.add(CSV.join(dimensionValues));
    }

    // Add overlay
    if (overlay != null) {
      if (groupBy != null) {
        throw new BadRequestException("Cannot specify both overlay and groupBy");
      }

      // Parse overlay
      String overlayUnit = overlay.substring(overlay.length() - 1);
      int overlaySize = Integer.valueOf(overlay.substring(0, overlay.length() - 1));

      // Get overlay
      DateTime overlayStart;
      DateTime overlayEnd;
      if ("d".equals(overlayUnit)) {
        overlayStart = startTime.minusDays(overlaySize);
        overlayEnd = endTime.minusDays(overlaySize);
      } else if ("w".equals(overlayUnit)) {
        overlayStart = startTime.minusWeeks(overlaySize);
        overlayEnd = endTime.minusWeeks(overlaySize);
      } else if ("m".equals(overlayUnit)) {
        overlayStart = startTime.minusMonths(overlaySize);
        overlayEnd = endTime.minusMonths(overlaySize);
      } else if ("y".equals(overlayUnit)) {
        overlayStart = startTime.minusYears(overlaySize);
        overlayEnd = endTime.minusYears(overlaySize);
      } else {
        throw new BadRequestException("Invalid overlay unit " + overlayUnit);
      }

      long shiftMillis = endTime.getMillis() - overlayEnd.getMillis();
      ThirdEyeRequest overlayReq = new ThirdEyeRequestBuilder().setCollection(collection)
          .setMetricFunction(metricFunction).setStartTimeInclusive(overlayStart).setEndTime(overlayEnd)
          .setDimensionValues(fixedValues).build();

      Map<DimensionKey, MetricTimeSeries> overlayRes = thirdEyeClient.execute(overlayReq);
      for (Map.Entry<DimensionKey, MetricTimeSeries> entry : overlayRes.entrySet()) {
        List<Map<String, Object>> overlaySeries =
            convertSeries(entry.getKey(), entry.getValue(), metric, null);
        overlaySeries = shiftSeries(overlaySeries, shiftMillis);
        data.add(overlaySeries);
      }
    }

    MetricsGraphicsTimeSeries timeSeries = new MetricsGraphicsTimeSeries();
    timeSeries.setTitle(metric + (groupBy == null || "".equals(groupBy) ? "" : " by " + groupBy));
    timeSeries.setDescription(request.toString());
    timeSeries.setData(data);
    timeSeries.setxAccessor("time");
    timeSeries.setyAccessor("value");

    // Get any anomalies
    List<AnomalyResult> anomalies;
    boolean isAnomalyIdsSpecified = (anomalyIds != null && !anomalyIds.isEmpty());
    if (functionId != null && isAnomalyIdsSpecified) {
      throw new BadRequestException("Cannot specify both functionId and anomalyIds");
    } else if (functionId != null) {
      anomalies = resultDAO.findAllByCollectionTimeFunctionIdAndMetric(collection, metric,
          functionId, startTime, endTime);
    } else if (isAnomalyIdsSpecified) {
      anomalies = new ArrayList<>(anomalyIds.size());
      for (Long anomalyId : anomalyIds) {
        AnomalyResult anomaly = resultDAO.findById(anomalyId);
        if (anomaly != null) {
          anomalies.add(anomaly);
        }
      }
    } else {
      anomalies =
          resultDAO.findAllByCollectionTimeAndMetric(collection, metric, startTime, endTime);
    }

    StarTreeConfig config = thirdEyeClient.getStarTreeConfig(collection);
    if (groupBy == null) {
      // Only show values that match current combination
      List<AnomalyResult> filtered = new ArrayList<>();
      for (AnomalyResult anomaly : anomalies) {
        String[] dimensions = anomaly.getDimensions().split(",");
        // TODO what to do if the dimension key has changed?
        boolean matches = true;
        for (int i = 0; i < config.getDimensions().size(); i++) {
          DimensionSpec spec = config.getDimensions().get(i);
          if (!fixedValues.containsKey(spec.getName()) && !"*".equals(dimensions[i])) {
            matches = false;
            break;
          } else if (fixedValues.containsKey(spec.getName())
              && !fixedValues.get(spec.getName()).contains(dimensions[i])) {
            matches = false;
            break;
          }
        }

        if (matches) {
          filtered.add(anomaly);
        }
      }
      anomalies = filtered;
    } else {
      // If there's a group by, only show those for the visible dimension values
      int idx = -1;
      for (int i = 0; i < config.getDimensions().size(); i++) {
        if (config.getDimensions().get(i).getName().equals(groupBy)) {
          idx = i;
          break;
        }
      }

      // Get the represented values from the time series
      Set<String> representedValues = new HashSet<>();
      for (DimensionKey key : sortedAndFilteredKeys) {
        representedValues.add(key.getDimensionValues()[idx]);
      }

      // Filter the anomalies
      List<AnomalyResult> filtered = new ArrayList<>();
      for (AnomalyResult anomaly : anomalies) {
        String[] dimensions = anomaly.getDimensions().split(",");
        if (representedValues.contains(dimensions[idx])) {
          filtered.add(anomaly);
        }
      }
      anomalies = filtered;
    }

    // Add the anomaly markers
    List<Map<String, Object>> markers = new ArrayList<>();
    for (AnomalyResult anomaly : anomalies) {
      if (anomaly.getEndTimeUtc() == null) {
        // Point
        if (anomaly.getStartTimeUtc() >= startTime.getMillis()
            && anomaly.getStartTimeUtc() <= endTime.getMillis()) {
          markers.add(ImmutableMap.of("time", anomaly.getStartTimeUtc(), "label",
              (Object) anomaly.getId()));
        }
      } else {
        // Interval
        if (anomaly.getStartTimeUtc() >= startTime.getMillis()
            && anomaly.getStartTimeUtc() <= endTime.getMillis()) {
          markers.add(ImmutableMap.of("time", anomaly.getStartTimeUtc(), "label",
              (Object) ("START_" + anomaly.getId())));
        }
        if (anomaly.getEndTimeUtc() >= startTime.getMillis()
            && anomaly.getEndTimeUtc() <= endTime.getMillis()) {
          markers.add(ImmutableMap.of("time", anomaly.getEndTimeUtc(), "label",
              (Object) ("END_" + anomaly.getId())));
        }
      }
    }
    timeSeries.setMarkers(markers);
    timeSeries.setAnomalyResults(anomalies);

    if (legend.size() > 1) {
      timeSeries.setLegend(legend);
    }

    return timeSeries;
  }

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

  private ThirdEyeMetricFunction getMetricFunction(String metric, String bucketSize,
      String collection) throws Exception {
    if (bucketSize == null) {
      // default to data bucket size
      StarTreeConfig starTreeConfig = thirdEyeClient.getStarTreeConfig(collection);
      TimeGranularity bucket = starTreeConfig.getTime().getBucket();
      return new ThirdEyeMetricFunction(bucket, Collections.singletonList(metric));
    }
    return ThirdEyeMetricFunction.fromStr(String.format("AGGREGATE_%s(%s)", bucketSize, metric));
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
}
