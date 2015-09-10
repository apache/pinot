package com.linkedin.thirdeye.dashboard.util;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class UriUtils {
  public static Map<String, String> extractDimensionValues(MultivaluedMap<String, String> multiMap) {
    Map<String, String> dimensionValues = new TreeMap<>();

    for (Map.Entry<String, List<String>> entry : multiMap.entrySet()) {
      if (entry.getValue().size() != 1) {
        throw new WebApplicationException(
            new Exception("Must provide exactly one value for dimension " + entry.getKey()),
            Response.Status.BAD_REQUEST);
      }

      if (!entry.getKey().equals("funnels")) {
        dimensionValues.put(entry.getKey(), entry.getValue().get(0));
      }
    }

    return dimensionValues;
  }
}
