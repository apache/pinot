/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.anomaly.alert.grouping.auxiliary_info_provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimensionalAlertGroupAuxiliaryRecipientProvider extends BaseAlertGroupAuxiliaryInfoProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionalAlertGroupAuxiliaryRecipientProvider.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String AUXILIARY_RECIPIENTS_MAP_KEY = "auxiliaryRecipients";

  // The map from a dimension map to auxiliary email recipients
  private NavigableMap<DimensionMap, String> auxiliaryEmailRecipients = new TreeMap<>();

  // For testing purpose
  NavigableMap<DimensionMap, String> getAuxiliaryEmailRecipients() {
    return auxiliaryEmailRecipients;
  }

  @Override
  public void setParameters(Map<String, String> props) {
    super.setParameters(props);

    // Initialize the lookup table for recipients of different dimensions
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
  }

  @Override
  public AuxiliaryAlertGroupInfo getAlertGroupAuxiliaryInfo(DimensionMap dimensions, List<MergedAnomalyResultDTO> anomalyResultList) {
    if (dimensions == null || !auxiliaryEmailRecipients.containsKey(dimensions)) {
      return EMPTY_AUXILIARY_ALERT_GROUP_INFO;
    } else {
      AuxiliaryAlertGroupInfo info = new AuxiliaryAlertGroupInfo();
      info.setAuxiliaryRecipients(auxiliaryEmailRecipients.get(dimensions));
      return info;
    }
  }
}
