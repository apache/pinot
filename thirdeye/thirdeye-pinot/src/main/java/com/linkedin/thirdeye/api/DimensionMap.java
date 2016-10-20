package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;


/**
 * Wrapper class to represent key-value pairs of explored dimension name and value. The paris of explore dimensions are
 * sorted by their dimension names in ascending order.
 */
public class DimensionMap implements Comparable<DimensionMap> {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Dimension name to dimension value
  private SortedMap<String, String> dimensionMap = Collections.emptySortedMap();

  public SortedMap<String, String> getDimensionMap() {
    return Collections.unmodifiableSortedMap(dimensionMap);
  }

  public void setDimensionMap(Map<String, String> dimensionMap) {
    this.dimensionMap = new TreeMap<>(dimensionMap);
  }

  public static DimensionMap fromJsonString(String dimensionMapJsonString)
      throws IOException {
    return OBJECT_MAPPER.readValue(dimensionMapJsonString, DimensionMap.class);
  }

  /**
   * Returns a dimension map according to the given dimension key.
   *
   * Assume that the given dimension key is [US,front_page,*,*,...] and the schema dimension names are
   * [country,page_name,...], then this method return this dimension map: {country=US; page_name=front_page;}
   *
   * @param dimensionKey the dimension key to be used to covert to explored dimensions
   * @param schemaDimensionNames the schema dimension names
   * @return the key-value pair of dimension value and dimension name according to the given dimension key.
   */
  public static DimensionMap fromDimensionKey(DimensionKey dimensionKey, List<String> schemaDimensionNames) {
    DimensionMap dimensionMap = new DimensionMap();
    if (CollectionUtils.isNotEmpty(schemaDimensionNames)) {
      SortedMap<String, String> exploredDimensionMap = new TreeMap<>();
      String[] dimensionValues = dimensionKey.getDimensionValues();
      for (int i = 0; i < dimensionValues.length; ++i) {
        String dimensionValue = dimensionValues[i].trim();
        if (!dimensionValue.equals("") && !dimensionValue.equals("*")) {
          String dimensionName = schemaDimensionNames.get(i);
          exploredDimensionMap.put(dimensionName, dimensionValue);
        }
      }
      dimensionMap.setDimensionMap(exploredDimensionMap);
    } else {
      dimensionMap.setDimensionMap(Collections.emptySortedMap());
    }
    return dimensionMap;
  }

  public int size() {
    return dimensionMap.size();
  }

  @Override
  public String toString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return this.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DimensionMap) {
      DimensionMap otherDimensionMap = (DimensionMap) o;
      return ObjectUtils.equals(dimensionMap, otherDimensionMap.dimensionMap);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return dimensionMap.hashCode();
  }

  /**
   * Returns the compared result of the string representation of dimension maps.
   *
   * @param o the other explored dimensions.
   */
  @Override
  public int compareTo(DimensionMap o) {
    if (dimensionMap == null && o.dimensionMap == null) {
      return 0;
    } else if (dimensionMap == null) {
      return -1;
    } else if (o.dimensionMap == null) {
      return 1;
    }

    Iterator<Map.Entry<String, String>> thisIte = dimensionMap.entrySet().iterator();
    Iterator<Map.Entry<String, String>> thatIte = o.dimensionMap.entrySet().iterator();
    if (thisIte.hasNext()) {
      // o has a smaller map
      if (!thatIte.hasNext()) {
        return 1;
      }

      Map.Entry<String, String> thisEntry = thisIte.next();
      Map.Entry<String, String> thatEntry = thatIte.next();
      // Compare dimension name first
      int diff = ObjectUtils.compare(thisEntry.getKey(), thatEntry.getKey());
      if (diff != 0) {
        return diff;
      }
      // Compare dimension value afterwards
      diff = ObjectUtils.compare(thisEntry.getValue(), thatEntry.getValue());
      if (diff != 0) {
        return diff;
      }
    }

    // o has a larger map
    if (thatIte.hasNext()) {
      return -1;
    }

    return 0;
  }
}
