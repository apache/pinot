package com.linkedin.thirdeye.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class ThirdEyeUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeUtils.class);

  private static final String FILTER_VALUE_ASSIGNMENT_SEPARATOR = "=";
  private static final String FILTER_CLAUSE_SEPARATOR = ";";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static Multimap<String, String> getFilterSet(String filters) {
    Multimap<String, String> filterSet = ArrayListMultimap.create();
    if (StringUtils.isNotBlank(filters)) {
      String[] filterClauses = filters.split(FILTER_CLAUSE_SEPARATOR);
      for (String filterClause : filterClauses) {
        String[] values = filterClause.split(FILTER_VALUE_ASSIGNMENT_SEPARATOR, 2);
        if (values.length != 2) {
          throw new IllegalArgumentException("Filter values assigments should in pairs: " + filters);
        }
        filterSet.put(values[0], values[1]);
      }
    }
    return filterSet;
  }

  public static String convertMultiMapToJson(Multimap<String, String> multimap)
      throws JsonProcessingException {
    Map<String, Collection<String>> map = multimap.asMap();
    return OBJECT_MAPPER.writeValueAsString(map);
  }

  public static Multimap<String, String> convertToMultiMap(String json) {
    ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
    if (json == null) {
      return multimap;
    }
    try {
      TypeReference<Map<String, ArrayList<String>>> valueTypeRef =
          new TypeReference<Map<String, ArrayList<String>>>() {
          };
      Map<String, ArrayList<String>> map;

      map = OBJECT_MAPPER.readValue(json, valueTypeRef);
      for (Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
        ArrayList<String> valueList = entry.getValue();
        ArrayList<String> trimmedList = new ArrayList<>();
        for (String value : valueList) {
          trimmedList.add(value.trim());
        }
        multimap.putAll(entry.getKey(), trimmedList);
      }
      return multimap;
    } catch (IOException e) {
      LOG.error("Error parsing json:{} message:{}", json, e.getMessage());
    }
    return multimap;
  }

  public static String getSortedFiltersFromMultiMap(Multimap<String, String> filterMultiMap) {
    Set<String> filterKeySet = filterMultiMap.keySet();
    ArrayList<String> filterKeyList = new ArrayList<String>(filterKeySet);
    Collections.sort(filterKeyList);

    StringBuilder sb = new StringBuilder();
    for (String filterKey : filterKeyList) {
      ArrayList<String> values = new ArrayList<String>(filterMultiMap.get(filterKey));
      Collections.sort(values);
      for (String value : values) {
        sb.append(filterKey);
        sb.append(FILTER_VALUE_ASSIGNMENT_SEPARATOR);
        sb.append(value);
        sb.append(FILTER_CLAUSE_SEPARATOR);
      }
    }

    return StringUtils.chop(sb.toString());
  }

  public static String getSortedFilters(String filters) {
    Multimap<String, String> filterMultiMap = getFilterSet(filters);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }

    return sortedFilters;
  }

  public static String getSortedFiltersFromJson(String filterJson) {
    Multimap<String, String> filterMultiMap = convertToMultiMap(filterJson);
    String sortedFilters = getSortedFiltersFromMultiMap(filterMultiMap);

    if (StringUtils.isBlank(sortedFilters)) {
      return null;
    }

    return sortedFilters;
  }

  public static void main(String[] args) {
    String filters = "a=z;z=d;a=f;a=e;k=m;k=f;z=c;f=g;";
    System.out.println(getSortedFilters(filters));
  }

}
