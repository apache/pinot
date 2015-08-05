package com.linkedin.thirdeye.bootstrap.aggregation;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DimensionStats
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Map<String, Set<String>> dimensionValues;

  public DimensionStats() {
    dimensionValues = new HashMap<String, Set<String>>();
  }

  public Map<String, Set<String>> getDimensionValues() {
    return dimensionValues;
  }

  public void record(String dimensionName, String dimensionValue) {
    Set<String> values = dimensionValues.get(dimensionName);
    if (values == null) {
      values = new HashSet<String>();
      dimensionValues.put(dimensionName, values);
    }
    values.add(dimensionValue);
  }

  public byte[] toBytes() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static DimensionStats fromBytes(byte[] bytes) throws IOException
  {
    return OBJECT_MAPPER.readValue(bytes, DimensionStats.class);
  }

  public void update(DimensionStats dimensionStats) {
   for (Entry<String, Set<String>> entry : dimensionStats.getDimensionValues().entrySet()) {
     Set<String> values = dimensionValues.get(entry.getKey());
     if (values == null) {
       values = new HashSet<String>();
       dimensionValues.put(entry.getKey(), values);
     }
     values.addAll(entry.getValue());

   }

  }
}