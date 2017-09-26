package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.GroupedAnomalyResultsDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A grouper that groups anomalies by the specified dimensions. If "RollUp" option is enabled, then the groups that
 * contains only one anomaly will be grouped together with an empty group key. Each group of anomalies could have
 * a list of auxiliary email recipients, which will be append to the global email recipients.
 *
 * An usage example:
 * Assume that we have four anomalies, whose dimensions are enclosed in brackets: a1={D1=G1, D2=V1}, a2={D1=G1, D2=V2},
 * a3={D1=G2, D2=V3}, and a4={D1=G3, D2=V4}. We further assume that we want to group by dimension D1 with roll up
 * enabled, then this grouper returns this grouped result:
 * groupKey={D1=G1} : a1, a2
 * groupKey={} : a3, a4
 *
 * User could assign the auxiliary email recipients as follows and retrieved through group keys:
 *     {{D1=G1}:"group1AuxiliaryRecipents.com",{}:"rollUp.com"}
 */
public class DimensionalAlertGrouper extends BaseAlertGrouper {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionalAlertGrouper.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Used when the user does not specify any dimensions to group by
  private static final DummyAlertGrouper DUMMY_ALERT_GROUPER = new DummyAlertGrouper();

  public static final String GROUP_BY_KEY = "dimensions";
  public static final String ROLL_UP_SINGLE_DIMENSION_KEY = "rollUp";
  public static final String GROUP_BY_SEPARATOR = ",";

  // The dimension names to group the anomalies (e.g., country, page_name)
  private List<String> groupByDimensions = new ArrayList<>();

  // Rollup the groups of anomaly, which contains only one anomaly, to a group
  private boolean doRollUp = false;

  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);
    // Initialize dimension names to be grouped by
    if (this.props.containsKey(GROUP_BY_KEY)) {
      String[] dimensions = this.props.get(GROUP_BY_KEY).split(GROUP_BY_SEPARATOR);
      for (String dimension : dimensions) {
        groupByDimensions.add(dimension.trim());
      }
    }
    // Initialize roll up parameter
    if (props.containsKey(ROLL_UP_SINGLE_DIMENSION_KEY)) {
      String doRollUpString = props.get(ROLL_UP_SINGLE_DIMENSION_KEY);
      try {
        doRollUp = Boolean.parseBoolean(doRollUpString);
      } catch (Exception e) {
        LOG.error("Failed to read RollUp parameter; RollUp is set to {}", doRollUp);
      }
    }
  }

  @Override
  public Map<DimensionMap, GroupedAnomalyResultsDTO> group(List<MergedAnomalyResultDTO> anomalyResults) {
    if (CollectionUtils.isEmpty(groupByDimensions)) {
      return DUMMY_ALERT_GROUPER.group(anomalyResults);
    } else {
      Map<DimensionMap, GroupedAnomalyResultsDTO> groupedAnomaliesMap = new HashMap<>();
      for (MergedAnomalyResultDTO anomalyResult : anomalyResults) {
        DimensionMap anomalyDimensionMap = anomalyResult.getDimensions();
        DimensionMap alertGroupKey = this.constructGroupKey(anomalyDimensionMap);
        if (groupedAnomaliesMap.containsKey(alertGroupKey)) {
          GroupedAnomalyResultsDTO groupedAnomalyResults = groupedAnomaliesMap.get(alertGroupKey);
          groupedAnomalyResults.getAnomalyResults().add(anomalyResult);
        } else {
          GroupedAnomalyResultsDTO groupedAnomalyResults = new GroupedAnomalyResultsDTO();
          groupedAnomalyResults.getAnomalyResults().add(anomalyResult);
          groupedAnomaliesMap.put(alertGroupKey, groupedAnomalyResults);
        }
      }

      // Group all grouped anomalies that have only one anomaly
      if (doRollUp) {
        GroupedAnomalyResultsDTO rolledUpGroupedAnomaly = new GroupedAnomalyResultsDTO();
        List<MergedAnomalyResultDTO> groupedAnomalyList = rolledUpGroupedAnomaly.getAnomalyResults();
        Iterator<Map.Entry<DimensionMap, GroupedAnomalyResultsDTO>> iterator =
            groupedAnomaliesMap.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<DimensionMap, GroupedAnomalyResultsDTO> entry = iterator.next();
          List<MergedAnomalyResultDTO> groupedAnomalyResults = entry.getValue().getAnomalyResults();
          if (CollectionUtils.isNotEmpty(groupedAnomalyResults) && groupedAnomalyResults.size() == 1) {
            groupedAnomalyList.add(groupedAnomalyResults.get(0));
            iterator.remove();
          }
        }
        if (groupedAnomalyList.size() > 0) {
          groupedAnomaliesMap.put(new DimensionMap(), rolledUpGroupedAnomaly);
        }
      }

      return groupedAnomaliesMap;
    }
  }

  private DimensionMap constructGroupKey(DimensionMap rawKey) {
    DimensionMap alertGroupKey = new DimensionMap();
    for (String groupByDimensionName : groupByDimensions) {
      if (rawKey.containsKey(groupByDimensionName)) {
        alertGroupKey.put(groupByDimensionName, rawKey.get(groupByDimensionName));
      }
    }
    return alertGroupKey;
  }
}
