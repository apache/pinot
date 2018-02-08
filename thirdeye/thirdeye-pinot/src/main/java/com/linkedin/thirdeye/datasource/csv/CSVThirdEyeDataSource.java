package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class CSVThirdEyeDataSource implements ThirdEyeDataSource{
  Map<String, DataFrame> dataSources;

  public CSVThirdEyeDataSource(Map<String, DataFrame> dataSources) {
    this.dataSources = dataSources;
  }

  @Override
  public String getName() {
    return CSVThirdEyeDataSource.class.getSimpleName();
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    return null;
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return new ArrayList<>(dataSources.keySet());
  }

  @Override
  public void clear() throws Exception {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    return 0;
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return null;
  }
}
