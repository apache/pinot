package com.linkedin.thirdeye.dashboard.api;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;
import java.util.regex.Pattern;

public class DimensionGroupSpec {
  private String collection;
  private List<DimensionGroup> groups;

  public DimensionGroupSpec() {}

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public List<DimensionGroup> getGroups() {
    return groups;
  }

  public void setGroups(List<DimensionGroup> groups) {
    this.groups = groups;
  }

  @JsonIgnore
  public Map<String, Map<String, String>> getMapping() {
    Map<String, Map<String, String>> mapping = new HashMap<>();

    for (DimensionGroup group : groups) {
      if (group.getValues() != null) {
        Map<String, String> subMap = mapping.get(group.getDimension());
        if (subMap == null) {
          subMap = new HashMap<>();
          mapping.put(group.getDimension(), subMap);
        }

        for (String value : group.getValues()) {
          subMap.put(value, group.getName());
        }
      }
    }

    return mapping;
  }

  @JsonIgnore
  public Map<String, Map<Pattern, String>> getRegexMapping() {
    Map<String, Map<Pattern, String>> mapping = new HashMap<>();

    for (DimensionGroup group : groups) {
      if (group.getRegex() != null) {
        Map<Pattern, String> subMap = mapping.get(group.getDimension());
        if (subMap == null) {
          subMap = new HashMap<>();
          mapping.put(group.getDimension(), subMap);
        }

        Pattern pattern = Pattern.compile(group.getRegex());
        subMap.put(pattern, group.getName());
      }
    }

    return mapping;
  }

  @JsonIgnore
  public Map<String, Map<String, List<String>>> getReverseMapping() {
    Map<String, Map<String, List<String>>> reverse = new HashMap<>();

    for (DimensionGroup group : groups) {
      if (group.getValues() != null) {
        Map<String, List<String>> subMap = reverse.get(group.getDimension());
        if (subMap == null) {
          subMap = new HashMap<>();
          reverse.put(group.getDimension(), subMap);
        }

        // Groups will be unique
        subMap.put(group.getName(), group.getValues());
      }
    }

    return reverse;
  }

  public static DimensionGroupSpec emptySpec(String collection) {
    DimensionGroupSpec empty = new DimensionGroupSpec();
    empty.setCollection(collection);
    empty.setGroups(new ArrayList<DimensionGroup>(0));
    return empty;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionGroupSpec)) {
      return false;
    }
    DimensionGroupSpec s = (DimensionGroupSpec) o;
    return Objects.equals(s.getCollection(), collection) && Objects.equals(s.getGroups(), groups);
  }
}
