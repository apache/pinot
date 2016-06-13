/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.broker.helix;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;


/**
 * This is an ExternalViewChangeListener.
 * Will wake up when ExternalView changes, then update routing tables for assigned data resources.
 *
 *
 */
public class HelixBrokerRoutingTable implements ExternalViewChangeListener, InstanceConfigChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerRoutingTable.class);
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final String _instanceId;
  private final HelixManager _helixManager;
  private final Builder keyBuilder;

  public HelixBrokerRoutingTable(HelixExternalViewBasedRouting helixExternalViewBasedRouting, String instanceId,
      HelixManager helixManager) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
    _instanceId = instanceId;
    _helixManager = helixManager;
    keyBuilder = _helixManager.getHelixDataAccessor().keyBuilder();
  }

  @Override
  public synchronized void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    refresh();
  }

  private void refresh() {
    LOGGER.info("HelixBrokerRoutingTable.onExternalViewChange");

    PropertyKey externalViews = keyBuilder.externalViews();
    HelixDataAccessor helixDataAccessor = _helixManager.getHelixDataAccessor();
    List<ExternalView> externalViewList = helixDataAccessor.getChildValues(externalViews);

    List<InstanceConfig> instanceConfigList = helixDataAccessor.getChildValues(keyBuilder.instanceConfigs());

    Set<String> servingClusterList = getServingDataResource(externalViewList);
    for (ExternalView externalView : externalViewList) {
      String resourceName = externalView.getResourceName();
      if (servingClusterList.contains(resourceName)) {
        LOGGER.debug("Trying to update ExternalView for data resource : {}, ExternalView: {}",
            resourceName, externalView);
        _helixExternalViewBasedRouting.markDataResourceOnline(
            resourceName,
            HelixHelper.getExternalViewForResource(_helixManager.getClusterManagmentTool(),
                _helixManager.getClusterName(), resourceName), instanceConfigList);
      }
    }
  }

  private Set<String> getServingDataResource(List<ExternalView> externalViewList) {
    Set<String> servingDataResourceSet = new HashSet<String>();
    for (ExternalView externalView : externalViewList) {
      if (externalView.getResourceName().equals(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        Set<String> dataResources = externalView.getPartitionSet();
        for (String dataResource : dataResources) {
          Map<String, String> dataResourceToServingBrokerMap = externalView.getStateMap(dataResource);
          if (dataResourceToServingBrokerMap.containsKey(_instanceId)
              && "ONLINE".equals(dataResourceToServingBrokerMap.get(_instanceId))) {
            servingDataResourceSet.add(dataResource);
          }
        }
      }
    }
    LOGGER.debug("Current serving data resource : {}", Arrays.toString(servingDataResourceSet.toArray(new String[0])));
    return servingDataResourceSet;
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> instanceConfigs, NotificationContext context) {
    refresh();
  }
}
