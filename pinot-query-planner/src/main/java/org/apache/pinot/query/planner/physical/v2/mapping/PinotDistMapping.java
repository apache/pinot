package org.apache.pinot.query.planner.physical.v2.mapping;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Mapping specifically for Pinot Data Distribution and trait mapping. A mapping is defined for a source / destination
 * RelNode pair and is used to track how input fields are mapped to output fields.
 */
public class PinotDistMapping {
  private static final int DEFAULT_MAPPING_VALUE = -1;
  private final int _sourceCount;
  private final Map<Integer, Integer> _sourceToTargetMapping = new HashMap<>();

  public PinotDistMapping(int sourceCount) {
    _sourceCount = sourceCount;
    for (int i = 0; i < sourceCount; i++) {
      _sourceToTargetMapping.put(i, DEFAULT_MAPPING_VALUE);
    }
  }

  public static PinotDistMapping identity(int sourceCount) {
    PinotDistMapping mapping = new PinotDistMapping(sourceCount);
    for (int i = 0; i < sourceCount; i++) {
      mapping.set(i, i);
    }
    return mapping;
  }

  public int getSourceCount() {
    return _sourceCount;
  }

  public int getTarget(int source) {
    Preconditions.checkArgument(source >= 0 && source < _sourceCount, "Invalid source index: %s", source);
    Integer target = _sourceToTargetMapping.get(source);
    return target == null ? DEFAULT_MAPPING_VALUE : target;
  }

  public void set(int source, int target) {
    Preconditions.checkArgument(source >= 0 && source < _sourceCount, "Invalid source index: %s", source);
    _sourceToTargetMapping.put(source, target);
  }

  public List<Integer> getMappedKeys(List<Integer> existingKeys) {
    List<Integer> result = new ArrayList<>(existingKeys.size());
    for (int key : existingKeys) {
      Integer mappedKey = _sourceToTargetMapping.get(key);
      Preconditions.checkArgument(mappedKey != null,
          "Key %s not found in mapping with source count: %s", key, _sourceCount);
      if (mappedKey != DEFAULT_MAPPING_VALUE) {
        result.add(mappedKey);
      } else {
        return Collections.emptyList();
      }
    }
    return result;
  }
}
