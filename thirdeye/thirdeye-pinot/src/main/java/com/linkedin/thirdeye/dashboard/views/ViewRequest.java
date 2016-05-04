package com.linkedin.thirdeye.dashboard.views;


import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;

public interface ViewRequest {

  String getCollection();

  Multimap<String, String> getFilters();

  TimeGranularity getTimeGranularity();
}
