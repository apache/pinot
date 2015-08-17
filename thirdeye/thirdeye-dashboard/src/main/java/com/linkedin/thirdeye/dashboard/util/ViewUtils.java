package com.linkedin.thirdeye.dashboard.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.util.*;
import java.util.regex.Pattern;

public class ViewUtils {
  private static final Joiner OR_JOINER = Joiner.on(" OR ");
  private static final TypeReference<List<String>> LIST_TYPE_REFERENCE = new TypeReference<List<String>>(){};
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewUtils.class);

  public static Map<String, String> fillDimensionValues(CollectionSchema schema, Map<String, String> dimensionValues) {
    Map<String, String> filled = new TreeMap<>();
    for (String name : schema.getDimensions()) {
      String value = dimensionValues.get(name);
      if (value == null) {
        value = "*";
      }
      filled.put(name, value);
    }
    return filled;
  }

  public static Map<String, String> flattenDisjunctions(MultivaluedMap<String, String> dimensionValues) {
    Map<String, String> flattened = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : dimensionValues.entrySet()) {
      flattened.put(entry.getKey(), OR_JOINER.join(entry.getValue()));
    }
    return flattened;
  }

  public static Map<List<String>, Map<String, Number[]>> processDimensionGroups(
      QueryResult queryResult,
      ObjectMapper objectMapper,
      Map<String, Map<String, String>> dimensionGroupMap,
      Map<String, Map<Pattern, String>> dimensionRegexMap,
      String dimensionName) throws Exception {
    // Aggregate w.r.t. dimension groups
    Map<List<String>, Map<String, Number[]>> processedResult = new HashMap<>(queryResult.getData().size());
    for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      List<String> combination = objectMapper.readValue(entry.getKey(), LIST_TYPE_REFERENCE);
      Integer dimensionIdx = queryResult.getDimensions().indexOf(dimensionName);
      String value = combination.get(dimensionIdx);

      // Map to group
      boolean appliedExplicitMapping = false;
      if (dimensionGroupMap != null) {
        Map<String, String> mapping = dimensionGroupMap.get(dimensionName);
        if (mapping != null) {
          String groupValue = mapping.get(value);
          if (groupValue != null) {
            combination.set(dimensionIdx, groupValue);
            appliedExplicitMapping = true;
          }
        }
      }

      // Use regex (lower priority)
      if (!appliedExplicitMapping && dimensionRegexMap != null) {
        Map<Pattern, String> patterns = dimensionRegexMap.get(dimensionName);
        if (patterns != null) {
          int matches = 0;
          for (Map.Entry<Pattern, String> pattern : patterns.entrySet()) {
            if (pattern.getKey().matcher(value).find()) {
              matches++;
              combination.set(dimensionIdx, pattern.getValue());
            }
          }

          if (matches > 1) {
            LOGGER.warn("Multiple regexes match {}! {}", value, patterns);
          }
        }
      }

      Map<String, Number[]> existing = processedResult.get(combination);
      if (existing == null) {
        existing = new HashMap<>();
        processedResult.put(combination, existing);
      }

      for (Map.Entry<String, Number[]> point : entry.getValue().entrySet()) {
        Number[] values = existing.get(point.getKey());
        if (values == null) {
          values = new Number[point.getValue().length];
          Arrays.fill(values, 0);
          existing.put(point.getKey(), values);
        }

        Number[] increment = point.getValue();
        for (int i = 0; i < values.length; i++) {
          values[i] = values[i].doubleValue() + increment[i].doubleValue();
        }
      }
    }

    return processedResult;
  }
}
