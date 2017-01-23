package com.linkedin.thirdeye.anomaly.alert.util;

import com.linkedin.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.collections.MapUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Stateless class to provide util methods to help build data report
 */
public final class DataReportHelper {
  private static final String DIMENSION_VALUE_SEPARATOR = ", ";
  private static final String EQUALS = "=";

  public static final String DECIMAL_FORMATTER = "%+.1f";
  public static final String OVER_ALL = "OverAll";

  private static DataReportHelper INSTANCE = new DataReportHelper();

  private DataReportHelper() {
  }

  public static DataReportHelper getInstance() {
    return INSTANCE;
  }

  // report list metric vs groupByKey vs dimensionVal vs timeBucket vs values
  public List<MetricDimensionReport> getDimensionReportList(List<ContributorViewResponse> reports) {
    List<MetricDimensionReport> ultimateResult = new ArrayList<>();
    for (ContributorViewResponse report : reports) {
      MetricDimensionReport metricDimensionReport = new MetricDimensionReport();
      String metric = report.getMetrics().get(0);
      String groupByDimension = report.getDimensions().get(0);
      metricDimensionReport.setMetricName(metric);
      metricDimensionReport.setDimensionName(groupByDimension);
      int valIndex =
          report.getResponseData().getSchema().getColumnsToIndexMapping().get("percentageChange");
      int dimensionIndex =
          report.getResponseData().getSchema().getColumnsToIndexMapping().get("dimensionValue");

      int numDimensions = report.getDimensionValuesMap().get(groupByDimension).size();
      int numBuckets = report.getTimeBuckets().size();

      // this is dimension vs timeBucketValue map, this should be sorted based on first bucket value
      Map<String, Map<String, String>> dimensionValueMap = new LinkedHashMap<>();

      // Lets populate 'OverAll' contribution
      Map<String, String> overAllValueMap = new LinkedHashMap<>();
      populateOverAllValuesMap(report, overAllValueMap);
      dimensionValueMap.put(OVER_ALL, overAllValueMap);

      Map<String, Map<String, String>> dimensionValueMapUnordered = new HashMap<>();

      for (int p = 0; p < numDimensions; p++) {
        if (p == 0) {
          metricDimensionReport
              .setCurrentStartTime(report.getTimeBuckets().get(0).getCurrentStart());
          metricDimensionReport
              .setCurrentEndTime(report.getTimeBuckets().get(numBuckets - 1).getCurrentEnd());
          metricDimensionReport
              .setBaselineStartTime(report.getTimeBuckets().get(0).getBaselineStart());
          metricDimensionReport
              .setBaselineEndTime(report.getTimeBuckets().get(numBuckets - 1).getBaselineEnd());
        }

        // valueMap is timeBucket vs value map
        LinkedHashMap<String, String> valueMap = new LinkedHashMap<>();
        String currentDimension = "";
        for (int q = 0; q < numBuckets; q++) {
          int index = p * numBuckets + q;
          currentDimension = report.getResponseData().getResponseData().get(index)[dimensionIndex];
          valueMap.put(String.valueOf(report.getTimeBuckets().get(q).getCurrentStart()), String
              .format(DECIMAL_FORMATTER,
                  Double.valueOf(report.getResponseData().getResponseData().get(index)[valIndex])));
        }
        dimensionValueMapUnordered.put(currentDimension, valueMap);
      }

      orderDimensionValueMap(dimensionValueMapUnordered, dimensionValueMap,
          report.getDimensionValuesMap().get(groupByDimension));
      metricDimensionReport.setSubDimensionValueMap(dimensionValueMap);
      populateShareAndTotalMap(report, metricDimensionReport, metric, groupByDimension);
      ultimateResult.add(metricDimensionReport);
    }
    return ultimateResult;
  }

  private static void orderDimensionValueMap(Map<String, Map<String, String>> src,
      Map<String, Map<String, String>> target, List<String> orderedKeys) {
    for (String key : orderedKeys) {
      target.put(key, src.get(key));
    }
  }

  private static void populateShareAndTotalMap(ContributorViewResponse report,
      MetricDimensionReport metricDimensionReport, String metric, String dimension) {
    Map<String, Double> currentValueMap =
        report.getCurrentTotalMapPerDimensionValue().get(metric).get(dimension);
    Map<String, Double> baselineValueMap =
        report.getBaselineTotalMapPerDimensionValue().get(metric).get(dimension);
    Map<String, String> shareMap = new HashMap<>();
    Map<String, String> totalMap = new HashMap<>();
    Double totalCurrent = 0d;
    Double totalBaseline = 0d;
    for (Map.Entry<String, Double> entry : currentValueMap.entrySet()) {
      totalCurrent += entry.getValue();
    }
    for (Map.Entry<String, Double> entry : baselineValueMap.entrySet()) {
      totalBaseline += entry.getValue();
    }
    // set value for overall as a sub-dimension
    shareMap.put(OVER_ALL, "100");
    totalMap.put(OVER_ALL,
        String.format(DECIMAL_FORMATTER, computePercentage(totalBaseline, totalCurrent)));

    for (Map.Entry<String, Double> entry : currentValueMap.entrySet()) {
      String subDimension = entry.getKey();
      // Share formatter does not need sign
      shareMap.put(subDimension, String.format("%.1f", 100 * entry.getValue() / totalCurrent));

      totalMap.put(subDimension, String.format(DECIMAL_FORMATTER,
          computePercentage(baselineValueMap.get(subDimension),
              currentValueMap.get(subDimension))));
    }

    metricDimensionReport.setSubDimensionShareValueMap(shareMap);
    metricDimensionReport.setSubDimensionTotalValueMap(totalMap);
  }

  private static double computePercentage(double baseline, double current) {
    if (baseline == current) {
      return 0d;
    }
    if (baseline == 0) {
      return 100d;
    }
    return 100 * (current - baseline) / baseline;
  }

  private static void populateOverAllValuesMap(ContributorViewResponse report,
      Map<String, String> overAllValueChangeMap) {
    Map<Long, Double> timeBucketVsCurrentValueMap = new LinkedHashMap<>();
    Map<Long, Double> timeBucketVsBaselineValueMap = new LinkedHashMap<>();
    int currentValIndex =
        report.getResponseData().getSchema().getColumnsToIndexMapping().get("currentValue");
    int baselineValIndex =
        report.getResponseData().getSchema().getColumnsToIndexMapping().get("baselineValue");

    // this is dimension vs timeBucketValue map, this should be sorted based on first bucket value
    String groupByDimension = report.getDimensions().get(0);
    int numDimensions = report.getDimensionValuesMap().get(groupByDimension).size();
    int numBuckets = report.getTimeBuckets().size();

    for (int p = 0; p < numDimensions; p++) {
      for (int q = 0; q < numBuckets; q++) {
        int index = p * numBuckets + q;
        long currentTimeKey = report.getTimeBuckets().get(q).getCurrentStart();
        double currentVal =
            Double.valueOf(report.getResponseData().getResponseData().get(index)[currentValIndex]);
        double baselineVal =
            Double.valueOf(report.getResponseData().getResponseData().get(index)[baselineValIndex]);

        if (!timeBucketVsCurrentValueMap.containsKey(currentTimeKey)) {
          timeBucketVsCurrentValueMap.put(currentTimeKey, 0D);
          timeBucketVsBaselineValueMap.put(currentTimeKey, 0D);
        }
        timeBucketVsCurrentValueMap
            .put(currentTimeKey, timeBucketVsCurrentValueMap.get(currentTimeKey) + currentVal);
        timeBucketVsBaselineValueMap
            .put(currentTimeKey, timeBucketVsBaselineValueMap.get(currentTimeKey) + baselineVal);
      }
    }
    for (Map.Entry<Long, Double> entry : timeBucketVsCurrentValueMap.entrySet()) {
      Double currentTotal = timeBucketVsCurrentValueMap.get(entry.getKey());
      Double baselineTotal = timeBucketVsBaselineValueMap.get(entry.getKey());
      double percentageChange = 0d;
      if (baselineTotal != 0d) {
        percentageChange = 100 * (currentTotal - baselineTotal) / baselineTotal;
      }
      overAllValueChangeMap.put(entry.getKey().toString(),
          String.format(DECIMAL_FORMATTER,percentageChange));
    }
  }

  /**
   * Convert a map of "dimension map to merged anomalies" to a map of "human readable dimension string to merged
   * anomalies".
   *
   * The dimension map is converted as follows. Assume that we have a dimension map (in Json string):
   * {"country"="US","page_name"="front_page'}, then it is converted to this String: "country=US, page_name=front_page".
   *
   * @param groupedResults a map of dimensionMap to a group of merged anomaly results
   * @return a map of "human readable dimension string to merged anomalies"
   */
  public static Map<String, List<MergedAnomalyResultDTO>> convertToStringKeyBasedMap(
      Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults) {
    // Sorted by dimension name and value pairs
    Map<String, List<MergedAnomalyResultDTO>> freemarkerGroupedResults = new TreeMap<>();

    if (MapUtils.isNotEmpty(groupedResults)) {
      for (Map.Entry<DimensionMap, List<MergedAnomalyResultDTO>> entry : groupedResults.entrySet()) {
        DimensionMap dimensionMap = entry.getKey();
        String dimensionMapString;
        if (MapUtils.isNotEmpty(dimensionMap)) {
          StringBuilder sb = new StringBuilder();
          String dimensionValueSeparator = "";
          for (Map.Entry<String, String> dimensionMapEntry : dimensionMap.entrySet()) {
            sb.append(dimensionValueSeparator).append(dimensionMapEntry.getKey());
            sb.append(EQUALS).append(dimensionMapEntry.getValue());
            dimensionValueSeparator = DIMENSION_VALUE_SEPARATOR;
          }
          dimensionMapString = sb.toString();
        } else {
          dimensionMapString = "ALL";
        }
        freemarkerGroupedResults.put(dimensionMapString, entry.getValue());
      }
    }
    return freemarkerGroupedResults;
  }

  public static class DateFormatMethod implements TemplateMethodModelEx {
    private final DateTimeZone TZ;
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public DateFormatMethod(DateTimeZone timeZone) {
      this.TZ = timeZone;
    }

    @Override
    public Object exec(@SuppressWarnings("rawtypes") List arguments) throws TemplateModelException {
      if (arguments.size() != 1) {
        throw new TemplateModelException("Wrong arguments, expected single millisSinceEpoch");
      }
      TemplateNumberModel tnm = (TemplateNumberModel) arguments.get(0);
      if (tnm == null) {
        return null;
      }

      Long millisSinceEpoch = tnm.getAsNumber().longValue();
      DateTime date = new DateTime(millisSinceEpoch, TZ);
      return date.toString(DATE_PATTERN);
    }
  }
}
