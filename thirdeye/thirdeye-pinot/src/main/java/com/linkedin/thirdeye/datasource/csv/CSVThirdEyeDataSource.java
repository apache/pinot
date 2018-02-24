package com.linkedin.thirdeye.datasource.csv;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Grouping;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.ObjectSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
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
      try (InputStreamReader reader = new InputStreamReader(makeUrlFromPath(property.getValue()).openStream())) {
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
  public ThirdEyeResponse execute(final ThirdEyeRequest request) throws Exception {
    DataFrame df = new DataFrame();
    for(MetricFunction function : request.getMetricFunctions()){
      final String inputName = translator.translate(function.getMetricId());
      final String outputName = function.toString();

      final MetricAggFunction aggFunction = function.getFunctionName();
      if (aggFunction != MetricAggFunction.SUM) {
        throw new IllegalArgumentException(String.format("Aggregation function '%s' not supported yet.", aggFunction));
      }

      DataFrame data = dataSets.get(function.getDataset());

      if (request.getStartTimeInclusive() != null){
        data = data.filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return values[0] >= request.getStartTimeInclusive().getMillis();
          }
        }, COL_TIMESTAMP);
      }

      if (request.getEndTimeExclusive() != null) {
        data = data.filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return values[0] < request.getEndTimeExclusive().getMillis();
          }
        }, COL_TIMESTAMP);
      }

      if (request.getFilterSet() != null){
        Multimap<String, String> filters = request.getFilterSet();
        for (final Map.Entry<String, Collection<String>> filter : filters.asMap().entrySet()){
          data = data.filter(new Series.StringConditional() {
            @Override
            public boolean apply(String... values) {
              return filter.getValue().contains(values[0]);
            }
          }, filter.getKey());
        }

      }

      data = data.dropNull();

      //
      // with grouping
      //
      if(request.getGroupBy() != null && request.getGroupBy().size() != 0){
        Grouping.DataFrameGrouping dataFrameGrouping = data.groupByValue(request.getGroupBy());
        List<String> aggregationExps = new ArrayList<>();
        final String[] groupByColumns = request.getGroupBy().toArray(new String[0]);
        for (String groupByCol : groupByColumns){
          aggregationExps.add(groupByCol + ":first");
        }
        aggregationExps.add(inputName + ":sum");

        if (request.getGroupByTimeGranularity() != null){
          // group by both time granularity and column
          List<DataFrame.Tuple> tuples = dataFrameGrouping.aggregate(aggregationExps).getSeries().get("key").getObjects().toListTyped();
          for (final DataFrame.Tuple key : tuples){
            DataFrame filteredData = data.filter(new Series.StringConditional() {
              @Override
              public boolean apply(String... values) {
                for(int i = 0; i < groupByColumns.length; i++){
                  if (values[i] != key.getValues()[i]){
                    return false;
                  }
                }
                return true;
              }
            }, groupByColumns);
            filteredData = filteredData.dropNull().groupByInterval(COL_TIMESTAMP, request.getGroupByTimeGranularity().toMillis()).aggregate(aggregationExps);
            if(df.size() == 0){
              df = filteredData;
            } else {
              df = df.append(filteredData);
            }
          }
          df.renameSeries(inputName, outputName);

        } else {
          // group by columns only
          df = dataFrameGrouping.aggregate(aggregationExps);
          df.dropSeries("key");
          df.renameSeries(inputName, outputName);
        }

      //
      // without dimension grouping
      //
      } else {
        if (request.getGroupByTimeGranularity() != null){
          // group by time granularity only
          df = data.groupByInterval(COL_TIMESTAMP, request.getGroupByTimeGranularity().toMillis()).aggregate(inputName + ":sum");
          df.renameSeries(inputName, outputName);
        } else {
          // aggregation only
          df.addSeries(outputName, data.getDoubles(inputName).sum());
          df.addSeries(COL_TIMESTAMP, LongSeries.buildFrom(-1));
        }
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

  private URL makeUrlFromPath(String input){
      try {
        return new URL(input);
      } catch (MalformedURLException ignore) {
        // ignore
      }
      return this.getClass().getResource(input);
  }
}


