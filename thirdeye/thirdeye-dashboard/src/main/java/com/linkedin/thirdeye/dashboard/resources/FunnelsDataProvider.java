package com.linkedin.thirdeye.dashboard.resources;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.DimensionGroupSpec;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.api.funnel.CustomFunnelSpec;
import com.linkedin.thirdeye.dashboard.api.funnel.FunnelSpec;
import com.linkedin.thirdeye.dashboard.util.DataCache;
import com.linkedin.thirdeye.dashboard.util.QueryCache;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;
import com.linkedin.thirdeye.dashboard.views.CustomFunnelTabularView;
import com.linkedin.thirdeye.dashboard.views.FunnelHeatMapView;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class FunnelsDataProvider {
  private static String FUNNELS_CONFIG_FILE_NAME = "funnels.yml";

  private static final Logger LOG = LoggerFactory.getLogger(FunnelsDataProvider.class);
  private static final Joiner METRIC_FUNCTION_JOINER = Joiner.on(",");
  private static final TypeReference<List<String>> LIST_REF = new TypeReference<List<String>>() {
  };
  private final File funnelsRoot;
  private final String serverUri;
  private final QueryCache queryCache;
  private final Map<String, CustomFunnelSpec> funnelSpecsMap;

  public FunnelsDataProvider(File funnelsRoot, String serverUri, QueryCache queryCache, DataCache dataCache)
      throws JsonProcessingException {
    this.funnelsRoot = funnelsRoot;
    this.serverUri = serverUri;
    this.queryCache = queryCache;
    this.funnelSpecsMap = new HashMap<String, CustomFunnelSpec>();
    loadConfigs();
    LOG.info("loaded custom funnel configs with {} ", new ObjectMapper().writeValueAsString(funnelSpecsMap));
  }

  public List<FunnelHeatMapView> computeFunnelViews(String collection, String selectedFunnels, DateTime currentDateTime, MultivaluedMap<String, String> dimensionValues) throws Exception {
    String[] funnels = selectedFunnels.split(",");
    if (funnels.length == 0) {
      return null;
    }

    List<FunnelHeatMapView> funnelViews = new ArrayList<FunnelHeatMapView>();

    for (String funnel : funnels) {
      LOG.info("adding funnel views for collection, {}, with funnel name {}", collection, funnel);
      funnelViews.add(getFunnelDataFor(collection, funnel, currentDateTime.getYear(),
          currentDateTime.getMonthOfYear(), currentDateTime.getDayOfMonth(), dimensionValues));
    }

    return funnelViews;
  }

  public List<String> getFunnelNamesFor(String collection) {
    List<String> funnelNames = new ArrayList<String>();

    if (!funnelSpecsMap.containsKey(collection)) {
      return funnelNames;
    }

    for (FunnelSpec spec : funnelSpecsMap.get(collection).getFunnels().values()) {
      funnelNames.add(spec.getName());
    }

    return funnelNames;
  }
  // currently funnels will overlook the current granularity and baseline granularity
  // it will only present views for every hour within the 24 hour period
  // filter format will be dimName1:dimValue1;dimName2:dimValue2


  public FunnelHeatMapView getFunnelDataFor(
      String collection,
      String funnel,
      Integer year,
      Integer month,
      Integer day,
      MultivaluedMap<String, String> dimensionValuesMap) throws Exception {


    // TODO : {dpatel} : this entire flow is extremely similar to custom dashboards, we should merge them

    FunnelSpec spec = funnelSpecsMap.get(collection).getFunnels().get(funnel);

    // get current start and end time
    DateTime currentEnd = new DateTime(year, month, day, 0, 0);
    DateTime currentStart = currentEnd.minusDays(1);

    // get baseline start and end
    DateTime baselineEnd = currentEnd.minusDays(7);
    DateTime baselineStart = baselineEnd.minusDays(1);

    String metricFunction = "AGGREGATE_1_HOURS(" + METRIC_FUNCTION_JOINER.join(spec.getActualMetricNames()) + ")";

    DimensionGroupSpec dimSpec = DimensionGroupSpec.emptySpec(collection);

    Map<String, Map<String, List<String>>> dimensionGroups = DimensionGroupSpec.emptySpec(collection).getReverseMapping();


    String baselineSql = SqlUtils.getSql(metricFunction, collection, baselineStart, baselineEnd, dimensionValuesMap, dimensionGroups);
    String currentSql = SqlUtils.getSql(metricFunction, collection, currentStart, currentEnd, dimensionValuesMap, dimensionGroups);

    LOG.info("funnel queries for collection : {}, with name : {} ", collection, spec.getName());
    LOG.info("Generated SQL: {}", baselineSql);
    LOG.info("Generated SQL: {}", currentSql);

    // Query
    Future<QueryResult> baselineResult = queryCache.getQueryResultAsync(serverUri, baselineSql);
    Future<QueryResult> currentResult = queryCache.getQueryResultAsync(serverUri, currentSql);

    // Baseline data
    Map<Long, Number[]> baselineData = CustomDashboardResource.extractFunnelData(baselineResult.get());
    Map<Long, Number[]> currentData = CustomDashboardResource.extractFunnelData(currentResult.get());

    // Compose result
    List<Pair<Long, Number[]>> table = new ArrayList<>();
    DateTime currentCursor = new DateTime(currentStart.getMillis());
    DateTime baselineCursor = new DateTime(baselineStart.getMillis());
    while (currentCursor.compareTo(currentEnd) < 0 && baselineCursor.compareTo(baselineEnd) < 0) {
      // Get values for this time
      Number[] baselineValues = baselineData.get(baselineCursor.getMillis());
      Number[] currentValues = currentData.get(currentCursor.getMillis());
      long hourOfDay = currentCursor.getHourOfDay(); // same as baseline

      if (baselineValues == null || currentValues == null) {
        table.add(new Pair<Long, Number[]>(hourOfDay, null));
      } else {
        // Compute percent change
        Number[] change = new Number[baselineValues.length];
        for (int i = 0; i < baselineValues.length; i++) {
          if (baselineValues[i] == null || currentValues[i] == null || baselineValues[i].doubleValue() == 0.0) {
            change[i] = null; // i.e. N/A, or cannot compute ratio to baseline
          } else {
            change[i] = (currentValues[i].doubleValue() - baselineValues[i].doubleValue()) / baselineValues[i].doubleValue();
          }
        }

        // Store in table
        table.add(new Pair<>(hourOfDay, change));
      }

      // Increment
      currentCursor = currentCursor.plusHours(1);
      baselineCursor = baselineCursor.plusHours(1);
    }

    // Get mapping of metric name to index
    Map<String, Integer> metricNameToIndex = new HashMap<>();
    List<String> resultMetrics = baselineResult.get().getMetrics();
    for (int i = 0; i < resultMetrics.size(); i++) {
      metricNameToIndex.put(resultMetrics.get(i), i);
    }

    // Filter (since query result set will contain primitive metrics for each derived one)
    List<Pair<Long, Number[]>> filteredTable = new ArrayList<>();
    for (Pair<Long, Number[]> pair : table) {
      Number[] filtered = new Number[spec.getActualMetricNames().size()];
      for (int i = 0; i < spec.getActualMetricNames().size(); i++) {
        String metricName = spec.getActualMetricNames().get(i);
        Integer metricIdx = metricNameToIndex.get(metricName);
        if (pair.getSecond() == null) {
          filtered[i] = 0;
        } else {
          Number value = null;
          try {
            value = pair.getSecond()[metricIdx];
          } catch (Exception e) {
            LOG.error("", e);
          }
          filtered[i] = value;
        }
      }
      filteredTable.add(new Pair<>(pair.getFirst(), filtered));
    }

    return new FunnelHeatMapView(spec, filteredTable, currentEnd, baselineEnd);
  }

  // TODO : {dpatel : move this to config cache later, would have started with it but found that out late}
  private void loadConfigs() {
    // looping throuh all the dirs, finding one funnels file and loading it up
    ObjectMapper ymlReader = new ObjectMapper(new YAMLFactory());
    for (File f : funnelsRoot.listFiles()) {
      File funnelsFile = new File(f, FUNNELS_CONFIG_FILE_NAME);
      if (!funnelsFile.exists()) {
        LOG.warn("did not find funnels config file {} , in folder {}", FUNNELS_CONFIG_FILE_NAME, f.getName());
        continue;
      }

      try {
        CustomFunnelSpec spec = ymlReader.readValue(funnelsFile, CustomFunnelSpec.class);
        this.funnelSpecsMap.put(spec.getCollection(), spec);
      } catch (Exception e) {
        LOG.error("error loading the configFile", e);
      }
    }
  }
}
