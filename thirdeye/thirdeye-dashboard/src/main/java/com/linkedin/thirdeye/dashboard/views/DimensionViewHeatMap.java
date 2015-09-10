package com.linkedin.thirdeye.dashboard.views;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.dashboard.api.*;
import com.linkedin.thirdeye.dashboard.util.SnapshotUtils;
import com.linkedin.thirdeye.dashboard.util.ViewUtils;

import io.dropwizard.views.View;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

public class DimensionViewHeatMap extends View {
  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionViewHeatMap.class);
  private static final TypeReference<List<String>> LIST_TYPE_REFERENCE = new TypeReference<List<String>>(){};
  private final CollectionSchema schema;
  private final ObjectMapper objectMapper;
  private final List<HeatMap> heatMaps;
  private final List<String> metricNames;
  private final List<String> dimensionNames;
  private final Map<String, Map<String,Number>> metricGlobalStats = new HashMap<String, Map<String,Number>>();
  private final Map<String, Map<String, String>> dimensionGroupMap;
  private final Map<String, Map<Pattern, String>> dimensionRegexMap;
  
  public DimensionViewHeatMap(CollectionSchema schema,
                              ObjectMapper objectMapper,
                              Map<String, QueryResult> queryResults,
                              Map<String, Map<String, String>> dimensionGroupMap,
                              Map<String, Map<Pattern, String>> dimensionRegexMap) throws Exception {
    super("dimension/heat-map.ftl");
    this.schema = schema;
    this.objectMapper = objectMapper;
    this.dimensionGroupMap = dimensionGroupMap;
    this.dimensionRegexMap = dimensionRegexMap;
    this.heatMaps = new ArrayList<>();
    this.metricNames = new ArrayList<>();
    this.dimensionNames = new ArrayList<>();

    for (Map.Entry<String, QueryResult> entry : queryResults.entrySet()) {
      this.heatMaps.addAll(generateHeatMaps(entry.getKey(), entry.getValue()));
    }

    Collections.sort(heatMaps);
    for(HeatMap heatMap:heatMaps){
      if(!metricNames.contains(heatMap.getMetric())){
        metricNames.add(heatMap.getMetric());
      }
      if(!dimensionNames.contains(heatMap.getDimension())){
        dimensionNames.add(heatMap.getDimension());
      }
    }
  }

  public Map<String, Map<String, Number>> getMetricGlobalStats()
  {
    return metricGlobalStats;
  }

  public List<HeatMap> getHeatMaps() {
    return heatMaps;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  private List<HeatMap> generateHeatMaps(String dimension, QueryResult queryResult) throws Exception {
    int dimensionIdx = queryResult.getDimensions().indexOf(dimension);

    if (queryResult.getData().isEmpty()) {
      return Collections.emptyList();
    }

    // Aliases

    Map<String, String> metricAliases = new HashMap<>();
    for (int i = 0; i < schema.getMetrics().size(); i++) {
      metricAliases.put(schema.getMetrics().get(i), schema.getMetricAliases().get(i));
    }

    Map<String, String> dimensionAliases = new HashMap<>();
    for (int i = 0; i < schema.getDimensions().size(); i++) {
      dimensionAliases.put(schema.getDimensions().get(i), schema.getDimensionAliases().get(i));
    }

    // Snapshot
    Map<String, Set<String>> snapshotValues = new HashMap<>();
    for (String metricName : queryResult.getMetrics()) {
      snapshotValues.put(metricName, new HashSet<String>());
    }
   /* try {
      String[][] snapshot = SnapshotUtils.snapshot(2, queryResult); // show top 2 movers
      for (int i = 0; i < queryResult.getMetrics().size(); i++) {
        String[] snapshotCombinations = snapshot[i];
        String metricName = queryResult.getMetrics().get(i);
        for (String combinationString : snapshotCombinations) {
          if (SnapshotUtils.REST.equals(combinationString)) {
            continue;
          }
          List<String> combination = objectMapper.readValue(combinationString.getBytes(), LIST_TYPE_REFERENCE);
          String value = combination.get(dimensionIdx);
          snapshotValues.get(metricName).add(value);
        }
      }
      LOGGER.info("snapshotValues={}", snapshotValues);
    } catch (Exception e) {
      LOGGER.error("Error generating snapshot", e);
    }*/

    // Initialize metric info
    Map<String, List<HeatMapCell>> allCells = new HashMap<>();
    Map<String, DescriptiveStatistics> allBaselineStats = new HashMap<>();
    Map<String, DescriptiveStatistics> allCurrentStats = new HashMap<>();
    for (int i = 0; i < queryResult.getMetrics().size(); i++) {
      String metric = queryResult.getMetrics().get(i);
      allCells.put(metric, new ArrayList<HeatMapCell>());
      allBaselineStats.put(metric, new DescriptiveStatistics());
      allCurrentStats.put(metric, new DescriptiveStatistics());
    }

    // Aggregate w.r.t. dimension groups
    Map<List<String>, Map<String, Number[]>> processedResult = ViewUtils.processDimensionGroups(
        queryResult, objectMapper, dimensionGroupMap, dimensionRegexMap, dimension);

    for (Map.Entry<List<String>, Map<String, Number[]>> entry : processedResult.entrySet()) {
      List<String> combination = entry.getKey();
      String value = combination.get(dimensionIdx);

      // Find min / max times (for current / baseline)
      long minTime = -1;
      long maxTime = -1;
      for (String timeString : entry.getValue().keySet()) {
        long time = Long.valueOf(timeString);
        if (minTime == -1 || time < minTime) {
          minTime = time;
        }
        if (maxTime == -1 || time > maxTime) {
          maxTime = time;
        }
      }

      // Add local stats
      for (int i = 0; i < queryResult.getMetrics().size(); i++) {
        String metric = queryResult.getMetrics().get(i);
        Number baselineValue = getMetricValue(entry.getValue().get(String.valueOf(minTime)), i);
        Number currentValue = getMetricValue(entry.getValue().get(String.valueOf(maxTime)), i);
        
        if (baselineValue == null)
        {
          baselineValue = new Double(0);
        }
        if (currentValue == null)
        {
          currentValue = new Double(0);
        }
        allBaselineStats.get(metric).addValue(baselineValue.doubleValue());
        allCurrentStats.get(metric).addValue(currentValue.doubleValue());

        HeatMapCell cell = new HeatMapCell(objectMapper, value);
        cell.addStat(Stat.BASELINE_VALUE, baselineValue);
        cell.addStat(Stat.CURRENT_VALUE,currentValue);

        allCells.get(metric).add(cell);
      }
    }

    List<HeatMap> heatMaps = new ArrayList<>();
    for (Map.Entry<String, List<HeatMapCell>> entry : allCells.entrySet()) {
      String metric = entry.getKey();
      DescriptiveStatistics baselineStats = allBaselineStats.get(metric);
      DescriptiveStatistics currentStats = allCurrentStats.get(metric);
      NormalDistribution baselineDist = getDistribution(baselineStats);
      NormalDistribution currentDist = getDistribution(currentStats);
      List<HeatMapCell> cells = entry.getValue();
      Collections.sort(cells, new Comparator<HeatMapCell>()
          {
            @Override
            public int compare(HeatMapCell o1, HeatMapCell o2)
            {
              //get the current value
              Number number1 = o1.getStats().get(1);
              Number number2 = o2.getStats().get(1);
              double val1 = (number1 == null) ? 0 : number1.doubleValue();
              double val2 = (number2 == null) ? 0 : number2.doubleValue();
              return NumberUtils.compare(val2, val1);
            }
          });

      // Add global stats
      for (HeatMapCell cell : cells) {
        Number baseline = cell.getStats().get(0);
        Number current = cell.getStats().get(1);
        double currentValue = current == null ? 0 : current.doubleValue();
        double baselineValue = baseline == null ? 0 : baseline.doubleValue();

        // Baseline p-value
        if (baseline == null || baselineDist == null) {
          cell.addStat(Stat.BASELINE_CDF_VALUE, null);
        } else {
          cell.addStat(Stat.BASELINE_CDF_VALUE, baselineDist.cumulativeProbability(baseline.doubleValue()));
        }

        // Current p-value
        if (current == null || currentDist == null) {
          cell.addStat(Stat.CURRENT_CDF_VALUE, null);
        } else {
          cell.addStat(Stat.CURRENT_CDF_VALUE, currentDist.cumulativeProbability(current.doubleValue()));
        }

        //Baseline total
        if (Double.isNaN(baselineStats.getSum())) {
            cell.addStat(Stat.BASELINE_TOTAL,null);
        } else {
            cell.addStat(Stat.BASELINE_TOTAL,baselineStats.getSum());
        }

        //Current total
        if (Double.isNaN(baselineStats.getSum())) {
          cell.addStat(Stat.CURRENT_TOTAL,null);
        } else {
          cell.addStat(Stat.CURRENT_TOTAL,currentStats.getSum());
        }

        // Baseline ratio
        if (baselineStats.getSum() > 0) {
          double baselineRatio = baseline == null ? 0 : baseline.doubleValue() / baselineStats.getSum();
          cell.addStat(Stat.BASELINE_RATIO,baselineRatio);
        } else {
          cell.addStat(Stat.BASELINE_RATIO,null);
        }

        // Current ratio
        if (currentStats.getSum() > 0) {
          double currentRatio = current == null ? 0 : current.doubleValue() / currentStats.getSum();
          cell.addStat(Stat.CURRENT_RATIO, currentRatio);
        } else {
          cell.addStat(Stat.CURRENT_RATIO,null);
        }

        // Contribution difference
        if (baselineStats.getSum() > 0 && currentStats.getSum() > 0) {
          double currentContribution = current == null ? 0 : current.doubleValue() / currentStats.getSum();
          double baselineContribution = baseline == null ? 0 : baseline.doubleValue() / baselineStats.getSum();
          cell.addStat(Stat.CONTRIBUTION_DIFFERENCE,currentContribution - baselineContribution);
        } else {
          cell.addStat(Stat.CONTRIBUTION_DIFFERENCE,null);
        }

        // Volume difference
        if (baselineStats.getSum() > 0) {
          cell.addStat(Stat.VOLUME_DIFFERENCE,(currentValue - baselineValue) / baselineStats.getSum());
        } else {
          cell.addStat(Stat.VOLUME_DIFFERENCE, null);
        }
        
        // Delta change percent
        if (baselineValue > 0) {
          cell.addStat(Stat.DELTA_PERCENT_CHANGE,(currentValue - baselineValue) / baselineValue);
        } else {
          cell.addStat(Stat.DELTA_PERCENT_CHANGE, 0);
        }
        
        // Snapshot category (i.e. is one of the biggest movers)
        if (snapshotValues.get(metric).contains(cell.getValue())) {
          cell.addStat(Stat.SNAPSHOT_CATEGORY,1);
        } else {
          cell.addStat(Stat.SNAPSHOT_CATEGORY,0);
        }
    }

      heatMaps.add(new HeatMap(objectMapper,
          entry.getKey(),
          metricAliases.get(entry.getKey()),
          dimension,
          dimensionAliases.get(dimension),
          cells,
          Arrays.asList(
              Stat.BASELINE_VALUE.toString(),         //0
              Stat.CURRENT_VALUE.toString(),          //1
              Stat.BASELINE_CDF_VALUE.toString(),     //2
              Stat.CURRENT_CDF_VALUE.toString(),      //3
              Stat.BASELINE_TOTAL.toString(),         //4
              Stat.CURRENT_TOTAL.toString(),          //5
              Stat.BASELINE_RATIO.toString(),         //6
              Stat.CURRENT_RATIO.toString(),          //7
              Stat.CONTRIBUTION_DIFFERENCE.toString(),//8
              Stat.VOLUME_DIFFERENCE.toString(),      //9
              Stat.SNAPSHOT_CATEGORY.toString(),       //10
              Stat.DELTA_PERCENT_CHANGE.toString()    //11
          )));
    }
    for(String metric:queryResult.getMetrics()){
      Map<String, Number> map = new HashMap<String, Number>();
      double baselineTotal = allBaselineStats.get(metric).getSum();
      double currentTotal = allCurrentStats.get(metric).getSum();
      map.put(Stat.BASELINE_TOTAL.toString(), baselineTotal);
      
      map.put(Stat.CURRENT_TOTAL.toString(), currentTotal);
      double delta = currentTotal - baselineTotal;
      map.put(Stat.DELTA_ABSOLUTE_CHANGE.toString(), delta);
      if(baselineTotal > 0){
        map.put(Stat.DELTA_PERCENT_CHANGE.toString(), (delta/baselineTotal) * 100.0  );
      }
      metricGlobalStats.put(metric, map);
    }
    return heatMaps;
  }

  private static Number getMetricValue(Number[] metrics, int idx) {
    if (metrics == null) {
      return null;
    }
    return metrics[idx];
  }

  private static NormalDistribution getDistribution(DescriptiveStatistics stats) {
    NormalDistribution dist = null;
    try {
      dist = new NormalDistribution(stats.getMean(), stats.getStandardDeviation());
    } catch (Exception e) {
      LOGGER.warn("Could not get statistics", e);
    }
    return dist;
  }
}
