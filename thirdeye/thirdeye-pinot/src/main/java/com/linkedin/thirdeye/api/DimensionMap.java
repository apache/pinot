package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;


/**
 * Stores key-value pairs of dimension name and value. The paris are sorted by their dimension names in ascending order.
 *
 * To reduces the length of string to be stored in database, this class implements SortedMap<String, String> for
 * converting to/from Json string in Map format, i.e., instead of storing {"sortedDimensionMap":{"country":"US",
 * "page_name":"front_page"}}, we only need to store {"country":"US","page_name":"front_page"}.
 */
public class DimensionMap implements SortedMap<String, String>, Comparable<DimensionMap>, Serializable {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Dimension name to dimension value pairs, which are sorted by dimension names
  private SortedMap<String, String> sortedDimensionMap = new TreeMap<>();

  public DimensionMap() {
  }

  /**
   * (Backward compatibility) For cleaning old anomalies from database.
   *
   * TODO: Remove this constructor after old anomalies are deleted
   *
   * @param dimensionKeyString
   */
  public DimensionMap(String dimensionKeyString) {
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
      String[] dimensionValues = dimensionKey.getDimensionValues();
      for (int i = 0; i < dimensionValues.length; ++i) {
        String dimensionValue = dimensionValues[i].trim();
        if (!dimensionValue.equals("") && !dimensionValue.equals("*")) {
          String dimensionName = schemaDimensionNames.get(i);
          dimensionMap.put(dimensionName, dimensionValue);
        }
      }
    }
    return dimensionMap;
  }

  public String toJson()
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(this);
  }

  /**
   * Returns if this dimension map is a child of the given dimension map, i.e., the given dimension map is a subset
   * of this dimension map.
   *
   * @param that the given dimension map.
   * @return true if this dimension map is a child of the given dimension map.
   */
  public boolean isChild(DimensionMap that) {
    if (that == null) { // equals to an empty dimension map, which is the root level of all dimensions
      return true;
    } else if (that.size() < this.size()) {
      // parent dimension map must be a subset of this dimension map
      for (Entry<String, String> parentDimensionEntry : that.entrySet()) {
        String thisDimensionValue = this.get(parentDimensionEntry.getKey());
        if (!parentDimensionEntry.getValue().equals(thisDimensionValue)) {
          return false;
        }
      }
      return true;
    } else if (that.size() == this.size()) {
      return this.equals(that);
    } else {
      return false;
    }
  }

  /**
   * Returns a JSON string representation of this dimension map for {@link com.linkedin.thirdeye.datalayer.dao.GenericPojoDao}
   * to persistent the map to backend database.
   *
   * It returns the generic string representation of this dimension map if any exception occurs when generating the JSON
   * string. In that case, the constructor {@link DimensionMap(String)} will be invoked during the construction of that
   * dimension map.
   *
   * @return a JSON string representation of this dimension map for {@link com.linkedin.thirdeye.datalayer.dao.GenericPojoDao}
   * to persistent the map to backend database.
   */
  @Override
  public String toString() {
    try {
      return this.toJson();
    } catch (JsonProcessingException e) {
      return super.toString();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DimensionMap) {
      DimensionMap otherDimensionMap = (DimensionMap) o;
      return ObjectUtils.equals(sortedDimensionMap, otherDimensionMap.sortedDimensionMap);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return sortedDimensionMap.hashCode();
  }

  /**
   * Returns the compared result of the string representation of dimension maps.
   *
   * Examples:
   * 1. a={}, b={"K"="V"} --> a="", b="KV" --> a < b
   * 2. a={"K"="V"}, b={"K"="V"} --> a="KV", b="KV" --> a = b
   * 3. a={"K2"="V1"}, b={"K1"="V1","K2"="V2"} --> a="K2V1", b="K1V1K2V2" --> b < a
   *
   * @param o the dimension to compare to
   */
  @Override
  public int compareTo(DimensionMap o) {
    Iterator<Map.Entry<String, String>> thisIte = sortedDimensionMap.entrySet().iterator();
    Iterator<Map.Entry<String, String>> thatIte = o.sortedDimensionMap.entrySet().iterator();
    if (thisIte.hasNext()) {
      // o is a smaller map
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

    // o is a larger map
    if (thatIte.hasNext()) {
      return -1;
    }

    return 0;
  }

  @Override
  public Comparator<? super String> comparator() {
    return sortedDimensionMap.comparator();
  }

  @Override
  public SortedMap<String, String> subMap(String fromKey, String toKey) {
    return sortedDimensionMap.subMap(fromKey, toKey);
  }

  @Override
  public SortedMap<String, String> headMap(String toKey) {
    return sortedDimensionMap.headMap(toKey);
  }

  @Override
  public SortedMap<String, String> tailMap(String fromKey) {
    return sortedDimensionMap.tailMap(fromKey);
  }

  @Override
  public String firstKey() {
    return sortedDimensionMap.firstKey();
  }

  @Override
  public String lastKey() {
    return sortedDimensionMap.lastKey();
  }

  @Override
  public int size() {
    return sortedDimensionMap.size();
  }

  @Override
  public boolean isEmpty() {
    return sortedDimensionMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return sortedDimensionMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return sortedDimensionMap.containsValue(value);
  }

  @Override
  public String get(Object key) {
    return sortedDimensionMap.get(key);
  }

  @Override
  public String put(String key, String value) {
    return sortedDimensionMap.put(key, value);
  }

  @Override
  public String remove(Object key) {
    return sortedDimensionMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends String, ? extends String> m) {
    sortedDimensionMap.putAll(m);
  }

  @Override
  public void clear() {
    sortedDimensionMap.clear();
  }

  @Override
  public Set<String> keySet() {
    return sortedDimensionMap.keySet();
  }

  @Override
  public Collection<String> values() {
    return sortedDimensionMap.values();
  }

  @Override
  public Set<Entry<String, String>> entrySet() {
    return sortedDimensionMap.entrySet();
  }

}
