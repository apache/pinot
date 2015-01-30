package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.log4j.Logger;

import com.linkedin.pinot.routing.HelixExternalViewBasedRouting;


/**
 * This is an ExternalViewChangeListener.
 * Will wake up when ExternalView changes, then update routing tables for assigned data resources.
 * 
 * @author xiafu
 *
 */
public class HelixBrokerRoutingTable implements ExternalViewChangeListener {
  private static final Logger LOGGER = Logger.getLogger(HelixBrokerRoutingTable.class);
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final String _instanceId;

  public HelixBrokerRoutingTable(HelixExternalViewBasedRouting helixExternalViewBasedRouting, String instanceId) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
    _instanceId = instanceId;
  }

  @Override
  public synchronized void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    LOGGER.info("HelixBrokerRoutingTable.onExternalViewChange");
    Set<String> servingClusterList = getServingDataResource(externalViewList);
    for (ExternalView externalView : externalViewList) {
      String resourceName = externalView.getResourceName();
      if (servingClusterList.contains(resourceName)) {
        LOGGER.info("Trying to update ExternalView for data resource : " + resourceName + ", ExternalView: "
            + externalView);
        _helixExternalViewBasedRouting.markDataResourceOnline(resourceName, externalView);
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
          if (dataResourceToServingBrokerMap.containsKey(_instanceId)) {
            if (dataResourceToServingBrokerMap.get(_instanceId).equals("ONLINE")) {
              servingDataResourceSet.add(dataResource);
            }
          }
        }
      }
    }
    LOGGER.info("Current serving data resource : " + Arrays.toString(servingDataResourceSet.toArray(new String[0])));
    return servingDataResourceSet;
  }
}
