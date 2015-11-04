package com.linkedin.thirdeye.dashboard.resources;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.CollectionSchema;
import com.linkedin.thirdeye.dashboard.api.MetricDataRow;
import com.linkedin.thirdeye.dashboard.api.MetricTable;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;
import com.linkedin.thirdeye.dashboard.views.DimensionViewContributors;

import io.dropwizard.views.View;

/**
 * Provides data for Contributor View. This primarily involves data for metric totals as well as
 * dimensional splits for each metric + dimension.
 * @author jteoh
 */
public class ContributorDataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContributorDataProvider.class);

  private static final TypeReference<List<String>> LIST_TYPE_REF =
      new TypeReference<List<String>>() {
      };

  private final String serverUri;
  private final QueryCache queryCache;
  private final ObjectMapper objectMapper;

  public ContributorDataProvider(String serverUri, QueryCache queryCache,
      ObjectMapper objectMapper) {
    this.serverUri = serverUri;
    this.queryCache = queryCache;
    this.objectMapper = objectMapper;
  }

  /**
   * Sends the appropriate queries and uses the response to create contributor tables.
   */
  public View generateDimensionContributorView(String collection, String metricFunction,
      MultivaluedMap<String, String> selectedDimensions, UriInfo uriInfo, CollectionSchema schema,
      DateTime baselineStart, DateTime currentStart,
      Map<String, Map<String, List<String>>> reverseDimensionGroups)
          throws Exception, InterruptedException, ExecutionException {
    long intraPeriod = ViewUtils.getIntraPeriod(metricFunction);
    // Since the total view is based off the funnel view, set times to be consistent (ie start of
    // day, PT)
    currentStart = ViewUtils.standardizeToStartOfDayPT(currentStart);
    baselineStart = ViewUtils.standardizeToStartOfDayPT(baselineStart);

    String baselineTotalSql = SqlUtils.getSql(metricFunction, collection, baselineStart,
        baselineStart.plus(intraPeriod), selectedDimensions, reverseDimensionGroups);
    String currentTotalSql = SqlUtils.getSql(metricFunction, collection, currentStart,
        currentStart.plus(intraPeriod), selectedDimensions, reverseDimensionGroups);

    LOGGER.info("Generated SQL for contributor baseline total {}: {}", uriInfo.getRequestUri(),
        baselineTotalSql);
    LOGGER.info("Generated SQL for contributor current total {}: {}", uriInfo.getRequestUri(),
        currentTotalSql);
    Future<QueryResult> baselineTotalResultFuture =
        queryCache.getQueryResultAsync(serverUri, baselineTotalSql);
    Future<QueryResult> currentTotalResultFuture =
        queryCache.getQueryResultAsync(serverUri, currentTotalSql);

    Map<String, Future<QueryResult>> baselineResultFutures = new HashMap<>();
    Map<String, Future<QueryResult>> currentResultFutures = new HashMap<>();
    for (String dimension : schema.getDimensions()) {
      if (!selectedDimensions.containsKey(dimension)) {
        // Generate SQL
        selectedDimensions.put(dimension, Arrays.asList("!"));
        String baselineGroupBySql = SqlUtils.getSql(metricFunction, collection, baselineStart,
            baselineStart.plus(intraPeriod), selectedDimensions, reverseDimensionGroups);
        String currentGroupBySql = SqlUtils.getSql(metricFunction, collection, currentStart,
            currentStart.plus(intraPeriod), selectedDimensions, reverseDimensionGroups);
        LOGGER.info("Generated SQL for contributor baseline {}: {}", uriInfo.getRequestUri(),
            baselineGroupBySql);
        LOGGER.info("Generated SQL for contributor current {}: {}", uriInfo.getRequestUri(),
            currentGroupBySql);
        selectedDimensions.remove(dimension);

        // Query (in parallel)
        baselineResultFutures.put(dimension,
            queryCache.getQueryResultAsync(serverUri, baselineGroupBySql));
        currentResultFutures.put(dimension,
            queryCache.getQueryResultAsync(serverUri, currentGroupBySql));

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

    MetricTable totalRow = generateMetricTotalTable(baselineTotalResult, currentTotalResult,
        baselineStart.getMillis(), currentStart.getMillis(), intraPeriod);

    Map<String, Map<String, MetricTable>> tables =
        generateDimensionTables(totalRow, dimensionBaselineResults, dimensionCurrentResults,
            baselineStart.getMillis(), currentStart.getMillis(), intraPeriod);
    return new DimensionViewContributors(currentTotalResult.getMetrics(), totalRow, tables);
  }

  /** Retrieves a single MetricTable from the provided metric total result. */
  private MetricTable generateMetricTotalTable(QueryResult totalBaselineResult,
      QueryResult totalCurrentResult, long baselineStart, long currentStart, long intraPeriod) {
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
        currentStart, intraPeriod, baselineValues, currentValues);

    return totalRow;

  }

  /**
   * Generates MetricTables for each dimension+value combination, using <tt>totalRow</tt> as a
   * reference for filling in missing time buckets.
   */
  private Map<String, Map<String, MetricTable>> generateDimensionTables(MetricTable totalRow,
      Map<String, QueryResult> dimensionBaselineResults,
      Map<String, QueryResult> dimensionCurrentResults, long baselineStart, long currentStart,
      long intraPeriod) throws JsonParseException, JsonMappingException, IOException {

    Map<String, Map<String, MetricTable>> tables = new TreeMap<>();
    /*
     * MetricTables consist of metrics over a time range, which now compose a single row in the
     * contributor table.
     */

    for (String dimension : dimensionCurrentResults.keySet()) {

      Map<String, MetricTable> rows = new TreeMap<>(); // dimValue mapping

      QueryResult queryBaselineResult = dimensionBaselineResults.get(dimension);
      QueryResult queryCurrentResult = dimensionCurrentResults.get(dimension);
      int dimensionValueIndex = queryCurrentResult.getDimensions().indexOf(dimension);
      // If we need to limit the number of dimension values returned, that can be done here.
      for (String dimensionKey : queryCurrentResult.getData().keySet()) {
        List<String> dimensionValues =
            objectMapper.readValue(dimensionKey.getBytes(), LIST_TYPE_REF);
        String dimensionValue = dimensionValues.get(dimensionValueIndex);

        Map<String, Number[]> currentValues = queryCurrentResult.getData().get(dimensionKey);
        Map<String, Number[]> baselineValues = queryBaselineResult.getData().get(dimensionKey);

        MetricTable row = generateTableRowWithReference(queryBaselineResult, baselineStart,
            currentStart, intraPeriod, baselineValues, currentValues, totalRow.getRows());
        rows.put(dimensionValue, row);
      }
      tables.put(dimension, rows);
    }
    return tables;
  }

  /**
   * Returns a MetricTable generated from the input maps of Timestamp->Number[], with the given time
   * window.
   */
  private MetricTable generateTableRow(int metricCount, long baselineStart, long currentStart,
      long intraPeriod, Map<String, Number[]> baselineValues, Map<String, Number[]> currentValues)
          throws NumberFormatException {

    Map<Long, Number[]> convertedBaselineValues = convertTimestamps(baselineValues);
    Map<Long, Number[]> convertedCurrentValues = convertTimestamps(currentValues);
    List<MetricDataRow> entries = ViewUtils.extractMetricDataRows(convertedBaselineValues,
        convertedCurrentValues, currentStart, currentStart - baselineStart, intraPeriod);
    List<MetricDataRow> cumulativeEntries = ViewUtils.computeCumulativeRows(entries, metricCount);
    return new MetricTable(entries, cumulativeEntries);
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
      long current, long intraPeriod, Map<String, Number[]> baselineValues,
      Map<String, Number[]> currentValues, List<MetricDataRow> referenceRows)
          throws NumberFormatException {
    int metricCount = totalResult.getMetrics().size();
    MetricTable orig = generateTableRow(metricCount, baseline, current, intraPeriod, baselineValues,
        currentValues);

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
    List<MetricDataRow> updatedCumulativeRows =
        ViewUtils.computeCumulativeRows(updatedRows, metricCount);
    return new MetricTable(updatedRows, updatedCumulativeRows);
  }

}
