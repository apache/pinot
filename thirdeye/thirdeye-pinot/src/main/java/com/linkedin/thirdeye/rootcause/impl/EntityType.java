package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;


/**
 * Wrapper class for URN prefix based typing of Entity.
 */
public final class EntityType {
  private final String prefix;

  public String getPrefix() {
    return prefix;
  }

  public EntityType(String prefix) {
    if(!prefix.endsWith(":"))
      throw new IllegalArgumentException("Prefix must end with ':'");
    this.prefix = prefix;
  }

  /**
   * Returns the parameterized type as string urn. Attaches values in order. Also unwraps the last element
   * if provided as a Map or Multimap.
   *
   * @param values parameters, in order
   * @return formatted urn
   */
  public String formatURN(Object... values) {
    if (values.length <= 0) {
      return this.prefix;
    }

    if (values[values.length - 1] instanceof Map) {
      Map<String, String> tailValues = (Map<String, String>) values[values.length - 1];
      Object[] headValues = Arrays.copyOf(values, values.length - 1);
      String tail = tailValues.isEmpty() ? "" :  ":" + makeTail(tailValues.entrySet());
      return this.prefix + StringUtils.join(headValues, ":") + tail;
    }

    if (values[values.length - 1] instanceof Multimap) {
      Multimap<String, String> tailValues = (Multimap<String, String>) values[values.length - 1];
      Object[] headValues = Arrays.copyOf(values, values.length - 1);
      String tail = tailValues.isEmpty() ? "" :  ":" + makeTail(tailValues.entries());
      return this.prefix + StringUtils.join(headValues, ":") + tail;
    }

    return this.prefix + StringUtils.join(values, ":");
  }

  public boolean isType(String urn) {
    return urn.startsWith(this.prefix);
  }

  public boolean isType(Entity e) {
    return e.getUrn().startsWith(this.prefix);
  }

  private static String makeTail(Collection<Map.Entry<String, String>> entries) {
    List<String> parts = new ArrayList<>();

    List<Map.Entry<String, String>> sorted = new ArrayList<>(entries);
    Collections.sort(sorted, new Comparator<Map.Entry<String, String>>() {
      @Override
      public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
        final int key = o1.getKey().compareTo(o2.getKey());
        if (key != 0)
          return key;
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    for (Map.Entry<String, String> entry : entries) {
      parts.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
    }

    return StringUtils.join(parts, ":");
  }
}
