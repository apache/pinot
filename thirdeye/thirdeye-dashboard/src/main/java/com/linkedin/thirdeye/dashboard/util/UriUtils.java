package com.linkedin.thirdeye.dashboard.util;

import java.util.List;
import java.util.Map.Entry;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

public class UriUtils {
  private static final String FUNNELS_KEY = "funnels";

  public static Multimap<String, String> extractDimensionValues(UriInfo uriInfo) {
    MultivaluedMap<String, String> mvMap = uriInfo.getQueryParameters();
    ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
    for (Entry<String, List<String>> entry : mvMap.entrySet()) {
      if (!FUNNELS_KEY.equals(entry.getKey())) {
        builder.putAll(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  public static List<String> extractFunnels(UriInfo uriInfo) {
    return uriInfo.getQueryParameters().get(FUNNELS_KEY);
  }
}
