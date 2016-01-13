package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConstants;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionDictionary implements Serializable {
  private static final long serialVersionUID = -403250971215465050L;

  private Map<String, Map<String, Integer>> dictionary;
  private Map<String, Map<Integer, String>> reverseDictionary;

  public DimensionDictionary() {
  }

  public DimensionDictionary(Map<String, Map<String, Integer>> dictionary) {
    setDictionary(dictionary);
  }

  public void setDictionary(Map<String, Map<String, Integer>> dictionary) {
    this.dictionary = dictionary;
    this.reverseDictionary = getReverseDictionary(dictionary);
  }

  public Map<String, Map<String, Integer>> getDictionary() {
    return dictionary;
  }

  public Map<String, Map<Integer, String>> getReverseDictionary() {
    return reverseDictionary;
  }

  public Map<String, Integer> getDictionary(String dimensionName) {
    Map<String, Integer> specificDict = dictionary.get(dimensionName);
    if (specificDict == null) {
      throw new IllegalArgumentException("No dictionary for " + dimensionName);
    }
    return specificDict;
  }

  public Map<Integer, String> getReverseDictionary(String dimensionName) {
    Map<Integer, String> specificDict = reverseDictionary.get(dimensionName);
    if (specificDict == null) {
      throw new IllegalArgumentException("No dictionary for " + dimensionName);
    }
    return specificDict;
  }

  public Integer getValueId(String dimensionName, String dimensionValue) {
    Integer valueId = getDictionary(dimensionName).get(dimensionValue);
    if (valueId == null) {
      valueId = StarTreeConstants.OTHER_VALUE;
    }
    return valueId;
  }

  public String getDimensionValue(String dimensionName, Integer valueId) {
    String dimensionValue = getReverseDictionary(dimensionName).get(valueId);
    if (dimensionValue == null) {
      throw new IllegalArgumentException("No dimension value for " + dimensionName + ":" + valueId);
    }
    return dimensionValue;
  }

  public int[] translate(List<DimensionSpec> dimensions, DimensionKey dimensionKey) {
    int[] translated = new int[dimensions.size()];

    for (int i = 0; i < dimensions.size(); i++) {
      translated[i] = getValueId(dimensions.get(i).getName(), dimensionKey.getDimensionValues()[i]);
    }

    return translated;
  }

  public DimensionKey translate(List<DimensionSpec> dimensions, int[] dimensionKey) {
    String[] translated = new String[dimensions.size()];

    for (int i = 0; i < dimensions.size(); i++) {
      translated[i] = getDimensionValue(dimensions.get(i).getName(), dimensionKey[i]);
    }

    return new DimensionKey(translated);
  }

  private static Map<String, Map<Integer, String>> getReverseDictionary(
      Map<String, Map<String, Integer>> dictionary) {
    Map<String, Map<Integer, String>> reverseDictionary =
        new HashMap<String, Map<Integer, String>>();

    for (Map.Entry<String, Map<String, Integer>> e1 : dictionary.entrySet()) {
      reverseDictionary.put(e1.getKey(), new HashMap<Integer, String>());
      for (Map.Entry<String, Integer> e2 : e1.getValue().entrySet()) {
        reverseDictionary.get(e1.getKey()).put(e2.getValue(), e2.getKey());
      }
    }

    return reverseDictionary;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionDictionary)) {
      return false;
    }

    DimensionDictionary d = (DimensionDictionary) o;

    return dictionary.equals(d.getDictionary());
  }
}
