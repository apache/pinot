package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.util.List;
import java.util.Map;


public class CSVThirdEyeDataSource implements ThirdEyeDataSource{

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
    return null;
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
