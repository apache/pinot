package com.linkedin.thirdeye.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;

/**
 * Utility class to support building {@link ThirdEyeRequest} objects.
 */
public class ThirdEyeRequestUtils {

  private static final Joiner COMMA = Joiner.on(",");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Creates a request from the input parameters, specifying the start of the request as the
   * beginning of the first day specified in the time range (as opposed to the provided start time).
   * For a more generic use case, see {@link ThirdEyeRequestBuilder}.
   */
  public static ThirdEyeRequest buildRequest(String collection, String groupByDimension,
      Map<String, String> fixedDimensionValues, List<String> metricNames,
      TimeGranularity aggregationGranularity, TimeRange timeRange) throws Exception {
    DateTime start = new DateTime(timeRange.getStart(), DateTimeZone.UTC);
    // make the start time more generic
    start = start.withMillisOfDay(0);
    DateTime end = new DateTime(timeRange.getEnd(), DateTimeZone.UTC);
    ThirdEyeMetricFunction metricFunction =
        new ThirdEyeMetricFunction(aggregationGranularity, metricNames);
    return new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
        .setStartTimeInclusive(start).setEndTime(end).setDimensionValues(fixedDimensionValues)
        .setGroupBy(groupByDimension).build();
  }

  /**
   * Expands any aliased values in <tt>dimensionValues</tt> according to their provided values in
   * <tt>dimensionGroups</tt>, which should be of the form
   * dimension->dimensionValueAlias->[dimensionValues].
   */
  public static Multimap<String, String> expandDimensionGroups(
      Multimap<String, String> dimensionValues,
      Map<String, Multimap<String, String>> dimensionGroups) {
    if (dimensionValues == null) {
      return null;
    } else if (dimensionGroups == null || dimensionGroups.isEmpty()) {
      return dimensionValues;
    }
    ArrayListMultimap<String, String> map = ArrayListMultimap.create();
    for (String key : dimensionValues.keySet()) {
      Collection<String> values = dimensionValues.get(key);
      if (values != null && values.size() == 1
          && ThirdEyeRequest.GROUP_BY_VALUE.equals(values.iterator().next())) {
        // Part of group by clause
        continue;
      }
      Multimap<String, String> valueMapping = null;
      if (dimensionGroups != null) {
        valueMapping = dimensionGroups.get(key);
      }
      for (String value : values) {
        // add alias values if there is a group mapping
        if (valueMapping != null && valueMapping.containsValue(value)) {
          Collection<String> groupValues = valueMapping.get(value);
          if (groupValues != null) {
            map.putAll(key, groupValues);
          }
        } else {
          map.put(key, value);
        }
      }
    }
    // TODO test case?
    return map;
  }

  public static Multimap<String, String> toMultimap(Map<String, String> map) {
    if (map == null) {
      return ArrayListMultimap.create();
    }
    ArrayListMultimap<String, String> multimap = ArrayListMultimap.create(map.size(), 1);
    for (Map.Entry<String, String> entry : map.entrySet()) {
      multimap.put(entry.getKey(), entry.getValue());
    }
    return multimap;
  }

  // primitive debugging mechanism, ideally meant to consume the printed toString() output and
  // recreate the results.
  private static ThirdEyeRequest fromString(String requestObject) throws Exception {
    // trim outer object
    requestObject =
        requestObject.substring(requestObject.indexOf('{') + 1, requestObject.lastIndexOf('}'));
    ThirdEyeRequestBuilder builder = ThirdEyeRequest.newBuilder();
    for (String string : requestObject.split(", ")) {
      String[] split = string.split("=");
      String property = split[0];
      String value = split[1];
      if (property.equals("collection")) {
        builder.setCollection(value);
      } else if (property.equals("metricFunction")) {
        builder.setMetricFunction(value);
      } else if (property.equals("startTime")) {
        builder.setStartTimeInclusive(ISODateTimeFormat.dateTimeParser().parseDateTime(value));
      } else if (property.equals("endTime")) {
        builder.setEndTime(ISODateTimeFormat.dateTimeParser().parseDateTime(value));
      } else if (property.equals("dimensionValues")) {
        // TODO fix to correctly handle multimap inputs
        builder.setDimensionValues(MAPPER.readValue(value, Map.class));
      } else if (property.equals("groupBy")) {
        value = value.replaceAll("[\\[\\]]", "");
        List<String> names = Arrays.asList(value.split(","));
        builder.setGroupBy(names);
      } else if (property.equals("shouldGroupByTime")) {
        builder.setShouldGroupByTime(Boolean.parseBoolean(value));
      }
    }

    return builder.build();

  }

}
