package com.linkedin.thirdeye.reporting.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.reporting.api.TableSpec;

public class DimensionKeyUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };

  public static String createQueryKey(String[] filteredDimension) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.writeValueAsString(filteredDimension);
  }

  public static DimensionKey createDimensionKey(String dimension)
      throws JsonParseException, JsonMappingException, IOException {

    List<String> dimensionValues = OBJECT_MAPPER.readValue(dimension, LIST_TYPE_REF);
    String[] valueArray = new String[dimensionValues.size()];
    dimensionValues.toArray(valueArray);
    DimensionKey dimensionKey = new DimensionKey(valueArray);
    return dimensionKey;
  }

  public static Map<String, String> createDimensionValues(TableSpec tableSpec) {
    Map<String, String> dimensionValues = new HashMap<String, String>();
    if (tableSpec.getFixedDimensions() != null) {
      dimensionValues.putAll(tableSpec.getFixedDimensions());
    }
    return dimensionValues;
  }

}
