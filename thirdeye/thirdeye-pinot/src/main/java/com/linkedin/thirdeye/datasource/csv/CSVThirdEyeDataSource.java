package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
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
  Map<String, DataFrame> dataSets;

  public static CSVThirdEyeDataSource fromDataFrame(Map<String, DataFrame> dataSets) {
    return new CSVThirdEyeDataSource(new InputMap(dataSets));
  }

  public static CSVThirdEyeDataSource fromUrl(Map<String, URL> dataSets) throws Exception {
    Map<String, DataFrame> dataframes = new HashMap<>();
    for (Map.Entry<String, URL> source : dataSets.entrySet()) {
      try (InputStreamReader reader = new InputStreamReader(source.getValue().openStream())) {
        dataframes.put(source.getKey(), DataFrame.fromCsv(reader));
      }
    }

    return new CSVThirdEyeDataSource(new InputMap(dataframes));
  }

  CSVThirdEyeDataSource(InputMap dataSets) {
    this.dataSets = dataSets;
  }

  public CSVThirdEyeDataSource(Map<String, String> properties) throws Exception{
    Map<String, DataFrame> dataframes = new HashMap<>();

    for(Map.Entry<String, String> property: properties.entrySet()){
      try (InputStreamReader reader = new InputStreamReader(new URL(property.getValue()).openStream())) {
        dataframes.put(property.getKey(), DataFrame.fromCsv(reader));
      }
    }

    this.dataSets = dataframes;
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
      MetricConfigDTO configDTO = DAORegistry.getInstance().getMetricConfigDAO().findById(function.getMetricId());
      if(configDTO == null){
        throw new IllegalArgumentException(String.format("Can not find metric id %d", function.getMetricId()));
      }
      if (functionName == MetricAggFunction.SUM){
        Double sum = dataSets.get(function.getDataset()).getDoubles(configDTO.getName()).sum().doubleValue();
        df.addSeries(functionName.toString(), sum);
      }
      df.addSeries(COL_TIMESTAMP, LongSeries.buildFrom(-1));
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
    return new ArrayList<>(dataSets.keySet());
  }

  @Override
  public void clear() throws Exception {

  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    if (!dataSets.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    return dataSets.get(dataset).getLongs(COL_TIMESTAMP).max().longValue();
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    if (!dataSets.containsKey(dataset)) {
      throw new IllegalArgumentException();
    }
    Map<String, Series> data = dataSets.get(dataset).getSeries();
    Map<String, List<String>> output = new HashMap<>();
    for (Map.Entry<String, Series> entry : data.entrySet()){
      if(entry.getValue().type() == STRING){
        output.put(entry.getKey(), entry.getValue().unique().getStrings().toList());
      }
    }
    return output;
  }


  private static class InputMap extends HashMap<String, DataFrame>{
    InputMap(Map<String, DataFrame> map){
      super(map);
    }
  }
}


