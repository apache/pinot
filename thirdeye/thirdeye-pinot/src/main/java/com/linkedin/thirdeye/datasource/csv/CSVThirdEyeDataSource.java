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
  TranslateDelegator translator;

  public static CSVThirdEyeDataSource fromDataFrame(Map<String, DataFrame> dataSets, Map<Long, String> metricNameMap) {
    return new CSVThirdEyeDataSource(dataSets, metricNameMap);
  }

  public static CSVThirdEyeDataSource fromUrl(Map<String, URL> dataSets, Map<Long, String> metricNameMap) throws Exception {
    Map<String, DataFrame> dataframes = new HashMap<>();
    for (Map.Entry<String, URL> source : dataSets.entrySet()) {
      try (InputStreamReader reader = new InputStreamReader(source.getValue().openStream())) {
        dataframes.put(source.getKey(), DataFrame.fromCsv(reader));
      }
    }

    return new CSVThirdEyeDataSource(dataframes, metricNameMap);
  }

  CSVThirdEyeDataSource(Map<String, DataFrame> dataSets, Map<Long, String> metricNameMap) {
    this.dataSets = dataSets;
    this.translator = new StaticTranslator(metricNameMap);
  }

  public CSVThirdEyeDataSource(Map<String, String> properties) throws Exception{
    Map<String, DataFrame> dataframes = new HashMap<>();
    for(Map.Entry<String, String> property: properties.entrySet()){
      try (InputStreamReader reader = new InputStreamReader(new URL(property.getValue()).openStream())) {
        dataframes.put(property.getKey(), DataFrame.fromCsv(reader));
      }
    }

    this.dataSets = dataframes;
    this.translator = new DAOTranslator();

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
        Double sum = dataSets.get(function.getDataset()).getDoubles(translator.translate(function.getMetricId())).sum().doubleValue();
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

  private interface TranslateDelegator{
    String translate(Long metricId);
  }

  private static class DAOTranslator implements TranslateDelegator {
    @Override
    public String translate(Long metricId) {
      MetricConfigDTO configDTO = DAORegistry.getInstance().getMetricConfigDAO().findById(metricId);
      if(configDTO == null){
        throw new IllegalArgumentException(String.format("Can not find metric id %d", metricId));
      }
      return configDTO.getName();
    }
  }

  private static class StaticTranslator implements TranslateDelegator{

    Map<Long, String> staticMap;

    public StaticTranslator(Map<Long, String> staticMap) {
      this.staticMap = staticMap;
    }

    @Override
    public String translate(Long metricId) {
      return staticMap.get(metricId);
    }
  }
}


