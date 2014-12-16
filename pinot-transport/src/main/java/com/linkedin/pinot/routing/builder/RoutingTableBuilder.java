package com.linkedin.pinot.routing.builder;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;


/**
 * Interface for creating a list of ServerToSegmentSetMap based on ExternalView from helix.
 * 
 * @author xiafu
 *
 */
public interface RoutingTableBuilder {

  /**
   * @param configuration
   */
  void init(Configuration configuration);

  /**
   * @param resourceName
   * @param externalView
   * @return List of routing table used to 
   */
  List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String resourceName, ExternalView externalView);
}
