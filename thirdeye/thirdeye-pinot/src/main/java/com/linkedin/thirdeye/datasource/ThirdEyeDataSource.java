package com.linkedin.thirdeye.datasource;

import java.util.List;
import java.util.Map;

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

  /**
   * Returns max dateTime in millis for the dataset
   * @param dataset
   * @return
   * @throws Exception
   */
  long getMaxDataTime(String dataset) throws Exception;

  /**
   * Returns map of dimension name to dimension values for filters
   * @param dataset
   * @return dimension map
   * @throws Exception
   */
  Map<String, List<String>> getDimensionFilters(String dataset) throws Exception;

}
