package com.linkedin.thirdeye.anomaly.alert.grouping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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

public class DimensionalAlertGrouper extends BaseAlertGrouper<DimensionMap> {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionalAlertGrouper.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Used when the user does not specify any dimensions to group by
  private static final DummyAlertGrouper DUMMY_ALERT_GROUPER = new DummyAlertGrouper();

  public static final String GROUP_BY_KEY = "dimensions";
  public static final String AUXILIARY_RECIPIENTS_MAP_KEY = "auxiliaryRecipients";
  public static final String ROLL_UP_SINGLE_DIMENSION_KEY = "rollUp";
  public static final String GROUP_BY_SEPARATOR = ",";

  // The dimension names to group the anomalies (e.g., country, page_name)
  private List<String> groupByDimensions = new ArrayList<>();

  // The map from a dimension map to auxiliary email recipients
  private NavigableMap<DimensionMap, String> auxiliaryEmailRecipients = new TreeMap<>();

  // Rollup the groups of anomaly, which contains only one anomaly, to a group
  private boolean doRollUp = false;

  // For testing purpose
  NavigableMap<DimensionMap, String> getAuxiliaryEmailRecipients() {
    return auxiliaryEmailRecipients;
  }

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
    // Initialize the lookup table for overriding thresholds
    if (props.containsKey(AUXILIARY_RECIPIENTS_MAP_KEY)) {
      String recipientsJsonPayLoad = props.get(AUXILIARY_RECIPIENTS_MAP_KEY);
      try {
        Map<String, String> rawAuxiliaryRecipientsMap = OBJECT_MAPPER.readValue(recipientsJsonPayLoad, HashMap.class);
        for (Map.Entry<String, String> auxiliaryRecipientsEntry : rawAuxiliaryRecipientsMap.entrySet()) {
          DimensionMap dimensionMap = new DimensionMap(auxiliaryRecipientsEntry.getKey());
          String recipients = auxiliaryRecipientsEntry.getValue();
          auxiliaryEmailRecipients.put(dimensionMap, recipients);
        }
      } catch (IOException e) {
        LOG.error("Failed to reconstruct auxiliary recipients mappings from this json string: {}", recipientsJsonPayLoad);
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
  public Map<GroupKey<DimensionMap>, GroupedAnomalyResults> group(List<MergedAnomalyResultDTO> anomalyResults) {
    if (CollectionUtils.isEmpty(groupByDimensions)) {
      return DUMMY_ALERT_GROUPER.group(anomalyResults);
    } else {
      Map<GroupKey<DimensionMap>, GroupedAnomalyResults> groupedAnomaliesMap = new HashMap<>();
      for (MergedAnomalyResultDTO anomalyResult : anomalyResults) {
        DimensionMap anomalyDimensionMap = anomalyResult.getDimensions();
        GroupKey<DimensionMap> groupKey = this.constructGroupKey(anomalyDimensionMap);
        if (groupedAnomaliesMap.containsKey(groupKey)) {
          GroupedAnomalyResults groupedAnomalyResults = groupedAnomaliesMap.get(groupKey);
          groupedAnomalyResults.getAnomalyResults().add(anomalyResult);
        }
      }

      // Group all grouped anomalies that have only one anomaly
      if (doRollUp) {
        GroupedAnomalyResults rolledUpGroupedAnomaly = new GroupedAnomalyResults();
        List<MergedAnomalyResultDTO> groupedAnomalyList = rolledUpGroupedAnomaly.getAnomalyResults();
        Iterator<Map.Entry<GroupKey<DimensionMap>, GroupedAnomalyResults>> iterator =
            groupedAnomaliesMap.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<GroupKey<DimensionMap>, GroupedAnomalyResults> entry = iterator.next();
          List<MergedAnomalyResultDTO> groupedAnomalyResults = entry.getValue().getAnomalyResults();
          if (CollectionUtils.isNotEmpty(groupedAnomalyResults) && groupedAnomalyResults.size() == 1) {
            groupedAnomalyList.add(groupedAnomalyResults.get(0));
            iterator.remove();
          }
        }
        if (groupedAnomalyList.size() > 0) {
          GroupKey<DimensionMap> groupKey = new GroupKey<>(new DimensionMap());
          groupedAnomaliesMap.put(groupKey, rolledUpGroupedAnomaly);
        }
      }

      return groupedAnomaliesMap;
    }
  }

  @Override
  public String groupEmailRecipients(GroupKey<DimensionMap> groupKey) {
    DimensionMap dimensionMap = groupKey.getKey();
    if (auxiliaryEmailRecipients.containsKey(dimensionMap)) {
      return auxiliaryEmailRecipients.get(dimensionMap);
    } else {
      return "";
    }
  }

  @Override
  public GroupKey<DimensionMap> constructGroupKey(DimensionMap rawKey) {
    GroupKey<DimensionMap> groupKey = new GroupKey<>(new DimensionMap());
    DimensionMap groupKeyDimensionMap = groupKey.getKey();
    for (String groupByDimensionName : groupByDimensions) {
      if (rawKey.containsKey(groupByDimensionName)) {
        groupKeyDimensionMap.put(groupByDimensionName, rawKey.get(groupByDimensionName));
      }
    }
    return groupKey;
  }
}
