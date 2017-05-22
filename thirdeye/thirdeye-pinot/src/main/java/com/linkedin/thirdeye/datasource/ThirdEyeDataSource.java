package com.linkedin.thirdeye.datasource;

import java.util.List;

public interface ThirdEyeDataSource {

  /**
   * Returns simple name of the class
   */
  String getName();

  ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception;

  List<String> getDatasets() throws Exception;

  /** Clear any cached values. */
  void clear() throws Exception;

  void close() throws Exception;

  long getMaxDataTime(String dataset) throws Exception;

  String getDimensionFilters(String dataset) throws Exception;

}
