package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DimensionStoreMutableImpl implements DimensionStore {
  private final ConcurrentMap<DimensionKey, Integer> dimensionKeys;
  private final List<DimensionSpec> dimensions;
  private final AtomicInteger nextDocId; // for dimension combinations
  private final AtomicInteger nextValueId; // for dimension values
  private final DimensionDictionary dictionary;

  public DimensionStoreMutableImpl(List<DimensionSpec> dimensions) {
    this.dimensionKeys = new ConcurrentHashMap<DimensionKey, Integer>();
    this.dimensions = dimensions;
    this.nextDocId = new AtomicInteger();
    this.nextValueId = new AtomicInteger(StarTreeConstants.FIRST_VALUE);
    this.dictionary = new DimensionDictionary();
  }

  @Override
  public int getDimensionKeyCount() {
    return dimensionKeys.keySet().size();
  }

  @Override
  public DimensionDictionary getDictionary() {
    return dictionary;
  }

  @Override
  public List<DimensionKey> getDimensionKeys() {
    return new ArrayList<DimensionKey>(dimensionKeys.keySet());
  }

  @Override
  /** This implementation lazily instantiates dimension keys, so there will always be a match */
  public Map<DimensionKey, Integer> findMatchingKeys(DimensionKey dimensionKey) {
    Map<DimensionKey, Integer> matches = new HashMap<DimensionKey, Integer>();

    // Add this key if we haven't seen it
    Integer existingDocId = dimensionKeys.get(dimensionKey);
    if (existingDocId == null) {
      dimensionKeys.putIfAbsent(dimensionKey, nextDocId.getAndIncrement());

      // Update dictionary
      for (int i = 0; i < dimensions.size(); i++) {
        String name = dimensions.get(i).getName();
        String value = dimensionKey.getDimensionValues()[i];
        dictionary.add(name, value, nextValueId.getAndIncrement());
      }
    }

    // Find all matches
    for (Map.Entry<DimensionKey, Integer> entry : dimensionKeys.entrySet()) {
      if (dimensionKey.matches(entry.getKey())) {
        matches.put(entry.getKey(), entry.getValue());
      }
    }

    return matches;
  }
}
