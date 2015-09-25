package com.linkedin.thirdeye.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricsGraphicsTimeSeries;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import io.dropwizard.hibernate.UnitOfWork;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.*;

@Path("/time-series/metrics-graphics")
public class MetricsGraphicsTimeSeriesResource {
  private static final Joiner CSV = Joiner.on(", ");
  private final ThirdEyeClient thirdEyeClient;
  private final AnomalyResultDAO resultDAO;

  public MetricsGraphicsTimeSeriesResource(ThirdEyeClient thirdEyeClient, AnomalyResultDAO resultDAO) {
    this.thirdEyeClient = thirdEyeClient;
    this.resultDAO = resultDAO;
  }

  @GET
  @Path("/{collection}/{metric}/{startTimeISO}/{endTimeISO}")
  @UnitOfWork
  @Produces(MediaType.APPLICATION_JSON)
  public MetricsGraphicsTimeSeries getTimeSeries(
      @PathParam("collection") String collection,
      @PathParam("metric") String metric,
      @PathParam("startTimeISO") String startTimeISO,
      @PathParam("endTimeISO") String endTimeISO,
      @QueryParam("timeZone") String timeZone,
      @QueryParam("bucketSize") String bucketSize,
      @QueryParam("groupBy") String groupBy,
      @QueryParam("topK") @DefaultValue("5") Integer topK,
      @QueryParam("functionId") Long functionId,
      @Context UriInfo uriInfo) throws Exception {
    DateTime startTime = parseDateTime(startTimeISO, timeZone);
    DateTime endTime = parseDateTime(endTimeISO, timeZone);

    // Get time series data
    ThirdEyeRequest req = new ThirdEyeRequest()
        .setCollection(collection)
        .setMetricFunction(getMetricFunction(metric, bucketSize))
        .setStartTime(startTime)
        .setEndTime(endTime)
        .setDimensionValues(extractDimensionValues(uriInfo));

    if (groupBy != null) {
      req.setGroupBy(groupBy);
    }

    // Do query
    Map<DimensionKey, MetricTimeSeries> res = thirdEyeClient.execute(req);
    Map<DimensionKey, List<Map<String, Object>>> dataByDimensionKey = new HashMap<>(res.size());
    final Map<DimensionKey, Double> totalVolume = new HashMap<>(res.size());

    // Transform result
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : res.entrySet()) {
      MetricTimeSeries timeSeries = entry.getValue();
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
        for (long time = sortedTimes.get(0); time <= sortedTimes.get(sortedTimes.size() - 1); time += minDiff) {
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
        Double volume = totalVolume.get(entry.getKey());
        if (volume == null) {
          volume = 0.0;
        }
        totalVolume.put(entry.getKey(), volume + value.doubleValue());
      }
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

    MetricsGraphicsTimeSeries timeSeries = new MetricsGraphicsTimeSeries();
    timeSeries.setTitle(metric + (groupBy == null || "".equals(groupBy) ? "" : " by " + groupBy));
    timeSeries.setDescription(req.toSql());
    timeSeries.setData(data);
    timeSeries.setxAccessor("time");
    timeSeries.setyAccessor("value");

    // Get any anomalies
    List<AnomalyResult> anomalies;
    if (functionId == null) {
      anomalies = resultDAO.findAllByCollectionTimeAndMetric(collection, metric, startTime, endTime);
    } else {
      anomalies = resultDAO.findAllByCollectionTimeFunctionIdAndMetric(collection, metric, functionId, startTime, endTime);
    }
    List<Map<String, Object>> markers = new ArrayList<>();
    for (AnomalyResult anomaly : anomalies) {
      if (anomaly.getEndTimeUtc() == null) {
        // Point
        markers.add(ImmutableMap.of("time", anomaly.getStartTimeUtc(), "label", (Object) anomaly.getId()));
      } else {
        // Interval
        markers.add(ImmutableMap.of("time", anomaly.getStartTimeUtc(), "label", (Object) ("START_" + anomaly.getId())));
        markers.add(ImmutableMap.of("time", anomaly.getEndTimeUtc(), "label", (Object) ("END_" + anomaly.getId())));
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

    List<Map<String, Object>> data = objectMapper.readValue(
        ClassLoader.getSystemResourceAsStream("dummy-time-series.json"),
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
    DateTime dateTime = ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime(dateTimeString);
    if (timeZone != null) {
      dateTime = dateTime.toDateTime(DateTimeZone.forID(timeZone));
    }
    return dateTime;
  }

  private static String getMetricFunction(String metric, String bucketSize) {
    if (bucketSize == null) {
      return metric;
    }
    return String.format("AGGREGATE_%s(%s)", bucketSize, metric);
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
}
