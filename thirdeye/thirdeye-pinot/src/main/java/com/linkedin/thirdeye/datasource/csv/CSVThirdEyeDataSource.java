package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.linkedin.thirdeye.dataframe.Series.SeriesType.*;


public class CSVThirdEyeDataSource implements ThirdEyeDataSource{
  public static final String COL_TIMESTAMP = "timestamp";
  Map<String, DataFrame> dataSources;

  public static CSVThirdEyeDataSource fromDataFrame(Map<String, DataFrame> dataSources) {
    return new CSVThirdEyeDataSource(new HashMap<String, DataFrame>(dataSources));
  }

  public static CSVThirdEyeDataSource fromUrl(Map<String, URL> dataSources) throws Exception {
    Map<String, DataFrame> dataframes = new HashMap<>();
    for (Map.Entry<String, URL> source : dataSources.entrySet()) {
      try (InputStreamReader reader = new InputStreamReader(source.getValue().openStream())) {
        dataframes.put(source.getKey(), DataFrame.fromCsv(reader));
      }
    }

    return new CSVThirdEyeDataSource(dataframes);
  }

  CSVThirdEyeDataSource(Map<String, DataFrame> dataSources) {
    this.dataSources = dataSources;
  }

  @Override
  public String getName() {
    return CSVThirdEyeDataSource.class.getSimpleName();
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    DataFrame df = new DataFrame();
    for(MetricFunction function : request.getMetricFunctions()){
      MetricAggFunction functionName = function.getFunctionName();
      if (functionName == MetricAggFunction.SUM){
        Double sum = dataSources.get(request.getDataSource()).getDoubles(function.getMetricName()).sum().doubleValue();
        df.addSeries(functionName.toString(), sum);
      }
    }
    CSVThirdEyeResponse response = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        df
        );
    return response;
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
    if (!dataSources.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    return dataSources.get(dataset).getLongs(COL_TIMESTAMP).max().longValue();
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    if (!dataSources.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    Map<String, Series> data = dataSources.get(dataset).getSeries();
    Map<String, List<String>> output = new HashMap<>();
    for (Map.Entry<String, Series> entry : data.entrySet()){
      if(entry.getValue().type() == STRING){
        output.put(entry.getKey(), entry.getValue().unique().getStrings().toList());
      }
    }
    return output;
  }
}
