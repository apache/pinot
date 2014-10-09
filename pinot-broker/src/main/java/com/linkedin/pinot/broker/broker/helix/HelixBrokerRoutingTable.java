package com.linkedin.pinot.broker.broker.helix;

import java.util.List;

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

  public HelixBrokerRoutingTable(HelixExternalViewBasedRouting helixExternalViewBasedRouting) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    System.out.println("HelixBrokerRoutingTable.onExternalViewChange");
    LOGGER.info("HelixBrokerRoutingTable.onExternalViewChange");
    for (ExternalView externalView : externalViewList) {
      String resourceName = externalView.getResourceName();
      if (_helixExternalViewBasedRouting.contains(resourceName)) {
        LOGGER.info("Trying to update ExternalView for data resource : " + resourceName + ", ExternalView: "
            + externalView);
        System.out.println(externalView);
        _helixExternalViewBasedRouting.markDataResourceOnline(resourceName, externalView);
      }
    }
  }
}
