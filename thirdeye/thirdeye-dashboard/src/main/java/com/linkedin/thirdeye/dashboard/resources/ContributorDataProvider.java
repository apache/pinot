package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.dashboard.api.MetricDataRow;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.IntraPeriod;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import com.linkedin.thirdeye.dashboard.views.DimensionViewContributors;

import io.dropwizard.views.View;

/**
 * Provides data for Contributor View. This primarily involves data for metric totals as well as
 * dimensional splits for each metric + dimension.
 * @author jteoh
 */
public class ContributorDataProvider {
  public static final String OTHER_DIMENSION_VALUE = StarTreeConstants.OTHER;
  public static final String OTHER_DIMENSION_VALUE_MATCH_STR = "other";
  private static final Logger LOGGER = LoggerFactory.getLogger(ContributorDataProvider.class);

  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };

  private static final double MINIMUM_DIMENSION_VALUE_THRESHOLD = 0.01;

  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;

  public ContributorDataProvider(QueryCache queryCache, ObjectMapper objectMapper) {
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
  }

  /**
   * Sends the appropriate queries and uses the response to create contributor tables.
   */
  public View generateDimensionContributorView(String collection, String metricFunction,
      Multimap<String, String> selectedDimensions, List<String> dimensions, DateTime baselineStart,
      DateTime currentStart, Map<String, Multimap<String, String>> reverseDimensionGroups)
          throws Exception, InterruptedException, ExecutionException {
    IntraPeriod intraPeriod = ViewUtils.getIntraPeriod(metricFunction);
    long intraPeriodMillis = intraPeriod.getMillis();
    // Since the total view is based off the funnel view, set times to be consistent (ie start of
    // day, PT)
    baselineStart = ViewUtils.standardizeDate(baselineStart, intraPeriod);
    currentStart = ViewUtils.standardizeDate(currentStart, intraPeriod);

    long baselineOffset = currentStart.getMillis() - baselineStart.getMillis();
    Multimap<String, String> expandedDimensionValues =
        ThirdEyeRequestUtils.expandDimensionGroups(selectedDimensions, reverseDimensionGroups);
    ThirdEyeRequest baselineTotalReq =
        new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
            .setStartTimeInclusive(baselineStart).setEndTime(baselineStart.plus(intraPeriodMillis))
            .setDimensionValues(expandedDimensionValues).build();
    ThirdEyeRequest currentTotalReq =
        new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
            .setStartTimeInclusive(currentStart).setEndTime(currentStart.plus(intraPeriodMillis))
            .setDimensionValues(expandedDimensionValues).build();

    LOGGER.info("Generated request for contributor baseline total: {}", baselineTotalReq);
    LOGGER.info("Generated request for contributor current total: {}", currentTotalReq);
    Future<QueryResult> baselineTotalResultFuture =
        queryCache.getQueryResultAsync(baselineTotalReq);
    Future<QueryResult> currentTotalResultFuture = queryCache.getQueryResultAsync(currentTotalReq);

    Map<String, Future<QueryResult>> baselineResultFutures = new HashMap<>();
    Map<String, Future<QueryResult>> currentResultFutures = new HashMap<>();
    for (String dimension : dimensions) {
      // Dimensions should be filtered already, but there's an additional check here.
      if (!selectedDimensions.containsKey(dimension)) {
        // Generate requests
        ThirdEyeRequest baselineGroupByReq =
            new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
                .setStartTimeInclusive(baselineStart).setEndTime(baselineStart.plus(intraPeriodMillis))
                .setDimensionValues(expandedDimensionValues).setGroupBy(dimension).build();
        ThirdEyeRequest currentGroupByReq =
            new ThirdEyeRequestBuilder().setCollection(collection).setMetricFunction(metricFunction)
                .setStartTimeInclusive(currentStart).setEndTime(currentStart.plus(intraPeriodMillis))
                .setDimensionValues(expandedDimensionValues).setGroupBy(dimension).build();
        LOGGER.info("Generated request for contributor baseline {} : {}", dimension,
            baselineGroupByReq);
        LOGGER.info("Generated request for contributor current {}: {}", dimension,
            currentGroupByReq);

        // Query (in parallel)
        baselineResultFutures.put(dimension, queryCache.getQueryResultAsync(baselineGroupByReq));
        currentResultFutures.put(dimension, queryCache.getQueryResultAsync(currentGroupByReq));

      } else {
        LOGGER.warn(
            "Found overlap between dimensions to query and pre-selected dimensions: ({}:{})",
            dimension, selectedDimensions.get(dimension));
      }
    }

    // Wait for all queries
    QueryResult baselineTotalResult = baselineTotalResultFuture.get();
    QueryResult currentTotalResult = currentTotalResultFuture.get();
    Map<String, QueryResult> dimensionBaselineResults = new HashMap<>(baselineResultFutures.size());
    Map<String, QueryResult> dimensionCurrentResults = new HashMap<>(currentResultFutures.size());
    for (Map.Entry<String, Future<QueryResult>> entry : baselineResultFutures.entrySet()) {
      dimensionBaselineResults.put(entry.getKey(), entry.getValue().get());
    }
    for (Map.Entry<String, Future<QueryResult>> entry : currentResultFutures.entrySet()) {
      dimensionCurrentResults.put(entry.getKey(), entry.getValue().get());
    }

    Map<String, MetricTable> totalRow =
        generateMetricTotalTable(baselineTotalResult, currentTotalResult, baselineStart.getMillis(),
            currentStart.getMillis(), intraPeriodMillis, baselineOffset);
    List<String> metrics = currentTotalResult.getMetrics();
    Map<Pair<String, String>, Map<String, MetricTable>> tables = generateDimensionTables(metrics,
        totalRow, dimensionBaselineResults, dimensionCurrentResults, baselineStart.getMillis(),
        currentStart.getMillis(), intraPeriodMillis, baselineOffset);
    return new DimensionViewContributors(metrics, dimensions, totalRow, tables);
  }

  /** Retrieves a single MetricTable from the provided metric total result. */
  private Map<String, MetricTable> generateMetricTotalTable(QueryResult totalBaselineResult,
      QueryResult totalCurrentResult, long baselineStart, long currentStart, long intraPeriod,
      long baselineOffset) {
    if (totalBaselineResult.getData().size() != 1 || totalCurrentResult.getData().size() != 1) {
      LOGGER.error("Must have exactly one dimension key present for total result!");
    }
    Map<String, Number[]> baselineValues = null;
    Map<String, Number[]> currentValues = null;
    for (Entry<String, Map<String, Number[]>> dimensionEntry : totalBaselineResult.getData()
        .entrySet()) {
      baselineValues = dimensionEntry.getValue();
    }
    for (Entry<String, Map<String, Number[]>> dimensionEntry : totalCurrentResult.getData()
        .entrySet()) {
      currentValues = dimensionEntry.getValue();
    }

    MetricTable totalRow = generateTableRow(totalCurrentResult.getMetrics().size(), baselineStart,
        currentStart, intraPeriod, baselineOffset, baselineValues, currentValues);

    return expandMetrics(totalRow, totalCurrentResult.getMetrics());

  }

  /**
   * Generates MetricTables for each metric+dimension+dimensionValue combination, using
   * <tt>totalRow</tt> as a
   * reference for filling in missing time buckets. The key of the returned table is (metric,
   * dimension).<br/>
   * Any dimension values matching "?" or "other" (ignored case) are grouped into the "?" category.
   * The same applies for dimension values that do not meet the required threshold.
   */
  private Map<Pair<String, String>, Map<String, MetricTable>> generateDimensionTables(
      List<String> metrics, Map<String, MetricTable> totalRow,
      Map<String, QueryResult> dimensionBaselineResults,
      Map<String, QueryResult> dimensionCurrentResults, long baselineStart, long currentStart,
      long intraPeriod, long baselineOffset)
          throws JsonParseException, JsonMappingException, IOException {
    Map<Pair<String, String>, Map<String, MetricTable>> table = new HashMap<>(); // key=(metric,dimension),
    List<MetricDataRow> referenceRows = null; // used for filling in missing time buckets
    Map<String, Double> totalCounts = new HashMap<>();
    // initialize metric-dimension tables, generate total counts for each metric, and retrieve a
    // reference row for time buckets.
    for (String metric : metrics) {
      if (referenceRows == null) {
        referenceRows = totalRow.get(metric).getRows();
      }
      totalCounts.put(metric, getContributionCount(totalRow.get(metric)));
    }

    // Due to the way that data is retrieved from server, loop (dimension, dimensionValue, metric).
    for (String dimension : dimensionCurrentResults.keySet()) {
      // set up tracking maps
      Map<String, Map<String, Double>> weightsByMetric = new HashMap<>();
      Map<String, Map<String, MetricTable>> tablesByMetric = new HashMap<>();
      for (String metric : metrics) {
        weightsByMetric.put(metric, new HashMap<String, Double>());
        tablesByMetric.put(metric, new HashMap<String, MetricTable>());
      }

      QueryResult queryBaselineResult = dimensionBaselineResults.get(dimension);
      QueryResult queryCurrentResult = dimensionCurrentResults.get(dimension);
      int dimensionValueIndex = queryCurrentResult.getDimensions().indexOf(dimension);

      for (String dimensionValuesKey : queryCurrentResult.getData().keySet()) {
        List<String> dimensionValues =
            objectMapper.readValue(dimensionValuesKey.getBytes(), LIST_TYPE_REF);
        String dimensionValue = dimensionValues.get(dimensionValueIndex);

        Map<String, Number[]> currentValues = queryCurrentResult.getData().get(dimensionValuesKey);
        Map<String, Number[]> baselineValues =
            queryBaselineResult.getData().get(dimensionValuesKey);

        // fill in missing time buckets if needed.
        MetricTable row =
            generateTableRowWithReference(queryBaselineResult, baselineStart, currentStart,
                intraPeriod, baselineOffset, baselineValues, currentValues, referenceRows);
        Map<String, MetricTable> metricRows = expandMetrics(row, metrics);

        // Filter on contribution.
        for (String metric : metrics) {

          MetricTable metricRow = metricRows.get(metric);

          Map<String, MetricTable> tables = tablesByMetric.get(metric);
          Map<String, Double> weights = weightsByMetric.get(metric);

          double contributionCount = getContributionCount(metricRow);
          double totalCount = totalCounts.get(metric);
          double contribution = (contributionCount / totalCount);
          LOGGER.debug("Meets threshold? {} : {} / {} for {} {} {}",
              contribution >= MINIMUM_DIMENSION_VALUE_THRESHOLD, contributionCount, totalCount,
              metric, dimension, dimensionValue);

          if (OTHER_DIMENSION_VALUE.equals(dimensionValue)
              || OTHER_DIMENSION_VALUE_MATCH_STR.equalsIgnoreCase(dimensionValue)
              || contribution < MINIMUM_DIMENSION_VALUE_THRESHOLD) {
            // group into OTHER (?)
            MetricTable otherRow = tables.get(OTHER_DIMENSION_VALUE);
            if (otherRow != null) {
              LOGGER.debug("Adding {} ({}) to OTHER ({})", dimensionValue, contributionCount,
                  getContributionCount(otherRow));
              metricRow = mergeTables(metricRow, otherRow, 1);
            }
            dimensionValue = OTHER_DIMENSION_VALUE;
            // recompute contribution
            contributionCount = getContributionCount(metricRow);
            contribution = (contributionCount / totalCount);
          }
          weights.put(dimensionValue, contribution);
          tables.put(dimensionValue, metricRow);
        }
      }

      // Extract data by descending contribution and add it to result map.
      for (String metric : metrics) {
        LinkedHashMap<String, MetricTable> dimensionValueTable =
            new LinkedHashMap<String, MetricTable>();
        table.put(new Pair<String, String>(metric, dimension), dimensionValueTable);

        Map<String, Double> weights = weightsByMetric.get(metric);
        Map<String, MetricTable> tables = tablesByMetric.get(metric);

        // Sort by descending contribution
        List<Entry<String, Double>> sortedEntries = sortedEntriesByValue(weights);
        Collections.reverse(sortedEntries);
        // go through entries ordered by descending contribution
        for (Entry<String, Double> entry : sortedEntries) {
          String dimensionValue = entry.getKey();
          MetricTable metricRow = tables.get(dimensionValue);
          dimensionValueTable.put(dimensionValue, metricRow);
        }
      }
    }
    return table;

  }

  private <K, V extends Comparable<V>> List<Entry<K, V>> sortedEntriesByValue(Map<K, V> unsorted) {
    List<Entry<K, V>> entries = new ArrayList<>(unsorted.entrySet());
    Collections.sort(entries, new Comparator<Entry<K, V>>() {

      @Override
      public int compare(Entry<K, V> o1, Entry<K, V> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });
    return entries;
  }

  /**
   * Returns a MetricTable generated from the input maps of Timestamp->Number[], with the given time
   * window.
   */
  private MetricTable generateTableRow(int metricCount, long baselineStart, long currentStart,
      long baselineOffset, long intraPeriod, Map<String, Number[]> baselineValues,
      Map<String, Number[]> currentValues) throws NumberFormatException {

    Map<Long, Number[]> convertedBaselineValues = convertTimestamps(baselineValues);
    Map<Long, Number[]> convertedCurrentValues = convertTimestamps(currentValues);
    List<MetricDataRow> entries = ViewUtils.extractMetricDataRows(convertedBaselineValues,
        convertedCurrentValues, currentStart, baselineOffset, intraPeriod);
    return new MetricTable(entries, metricCount);
  }

  /**
   * Converts String timestamp keys to Longs.
   */
  private Map<Long, Number[]> convertTimestamps(Map<String, Number[]> values)
      throws NumberFormatException {
    if (values == null) {
      return Collections.emptyMap();
    }
    Map<Long, Number[]> convertedBaselineRows = new HashMap<>();
    for (Entry<String, Number[]> timeEntry : values.entrySet()) {
      String timestamp = timeEntry.getKey();
      Number[] metricValues = timeEntry.getValue();
      convertedBaselineRows.put(Long.valueOf(timestamp), metricValues);
    }
    return convertedBaselineRows;
  }

  /**
   * Returns a metricTable generated from the input map of Timestamp -> Number[], using
   * <tt>referenceRows</tt> to fill in missing time buckets.
   */
  private MetricTable generateTableRowWithReference(QueryResult totalResult, long baseline,
      long current, long intraPeriod, long baselineOffset, Map<String, Number[]> baselineValues,
      Map<String, Number[]> currentValues, List<MetricDataRow> referenceRows)
          throws NumberFormatException {
    int metricCount = totalResult.getMetrics().size();
    MetricTable orig = generateTableRow(metricCount, baseline, current, intraPeriod, baselineOffset,
        baselineValues, currentValues);

    Iterator<MetricDataRow> iter = orig.getRows().iterator();
    MetricDataRow origRow = (iter.hasNext() ? iter.next() : null);

    List<MetricDataRow> updatedRows = new LinkedList<>();
    // Iterate through referenceRows and find matching origRows.
    // This loop assumes both lists are in chronological order.
    for (MetricDataRow referenceRow : referenceRows) {
      DateTime refTime = referenceRow.getCurrentTime();
      if (origRow == null || origRow.getCurrentTime().isBefore(refTime)) {
        while (origRow == null || origRow.getCurrentTime().isBefore(refTime)) {
          if (iter.hasNext()) {
            origRow = iter.next();
          } else {
            break;
          }
        }
      }
      // guaranteed that origRow is either end of list or valid entry.
      MetricDataRow updatedRow = null;
      if (origRow == null) {
        // End of list - use dummy row use dummy row to fill in gap.
        updatedRow = new MetricDataRow(referenceRow.getBaselineTime(), null,
            referenceRow.getCurrentTime(), null);
      } else if (origRow.getCurrentTime().equals(refTime)) {
        // no changes required.
        updatedRow = origRow;
      } else {
        // No matching time bucket - use dummy row to fill in gap.
        updatedRow = new MetricDataRow(referenceRow.getBaselineTime(), null,
            referenceRow.getCurrentTime(), null);
      }
      updatedRows.add(updatedRow);
    }
    return new MetricTable(updatedRows, metricCount);
  }

  private Map<String, MetricTable> expandMetrics(MetricTable table, List<String> metrics) {
    HashMap<String, MetricTable> result = new HashMap<>();
    for (int i = 0; i < metrics.size(); i++) {
      String metric = metrics.get(i);
      List<MetricDataRow> metricRows = new LinkedList<>();
      for (MetricDataRow row : table.getRows()) {
        MetricDataRow metricRow = extractMetric(row, i);
        metricRows.add(metricRow);
      }
      result.put(metric, new MetricTable(metricRows, 1));
    }
    return result;
  }

  private MetricDataRow extractMetric(MetricDataRow row, int metricIndex) {
    Number[] baselineValue = retrieveSingleValue(row.getBaseline(), metricIndex);
    Number[] currentValue = retrieveSingleValue(row.getCurrent(), metricIndex);
    MetricDataRow metricRow =
        new MetricDataRow(row.getBaselineTime(), baselineValue, row.getCurrentTime(), currentValue);
    return metricRow;
  }

  private Number[] retrieveSingleValue(Number[] values, int index) {
    if (values == null) {
      return null;
    } else {
      return new Number[] {
          values[index]
      };
    }

  }

  /** Returns final value for last cumulative row's first metric. */
  private double getContributionCount(MetricTable table) {
    List<MetricDataRow> cumulativeRows = table.getCumulativeRows();
    if (cumulativeRows == null || cumulativeRows.isEmpty()) {
      return 0;
    }
    MetricDataRow finalRow = cumulativeRows.get(cumulativeRows.size() - 1);
    if (finalRow.getCurrent() != null) {
      return finalRow.getCurrent()[0].doubleValue();
    } else {
      return 0;
    }
  }

  private MetricTable mergeTables(MetricTable metricRow, MetricTable otherRow, int metricCount) {
    if (metricRow == null) {
      return otherRow;
    } else if (otherRow == null) {
      return metricRow;
    }
    Iterator<MetricDataRow> metricRowIter = metricRow.getRows().iterator();
    Iterator<MetricDataRow> otherRowIter = otherRow.getRows().iterator();
    List<MetricDataRow> resultRows = new LinkedList<>();
    while (metricRowIter.hasNext()) {
      MetricDataRow metric = metricRowIter.next();
      MetricDataRow other = otherRowIter.next();
      Number[] baseline = addValues(metric.getBaseline(), other.getBaseline());
      Number[] current = addValues(metric.getCurrent(), other.getCurrent());
      MetricDataRow resultRow =
          new MetricDataRow(other.getBaselineTime(), baseline, other.getCurrentTime(), current);
      resultRows.add(resultRow);
    }
    return new MetricTable(resultRows, metricCount);
  }

  private Number[] addValues(Number[] a, Number[] b) {
    if (a == null) {
      return b;
    } else if (b == null) {
      return a;
    }
    Number[] result = new Number[a.length];
    for (int i = 0; i < a.length; i++) {
      double aVal = (a[i] == null ? 0.0 : a[i].doubleValue());
      double bVal = (b[i] == null ? 0.0 : b[i].doubleValue());
      result[i] = aVal + bVal;
    }
    return result;
  }

}
