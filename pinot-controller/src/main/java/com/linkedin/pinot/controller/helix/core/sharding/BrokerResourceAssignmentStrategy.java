package com.linkedin.pinot.controller.helix.core.sharding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.helix.HelixAdmin;
import org.apache.log4j.Logger;

import com.linkedin.pinot.controller.api.pojos.BrokerDataResource;


/**
 * Random assign segment to instances.
 * 
 * @author xiafu
 *
 */
public class BrokerResourceAssignmentStrategy {

  private static final Logger LOGGER = Logger.getLogger(BrokerResourceAssignmentStrategy.class);

  /**
   * Get broker instances with a given tag and do random assignment.
   * 
   * @param helixAdmin
   * @param helixClusterName
   * @param brokerDataResource
   * @return
   */
  public static List<String> getRandomAssignedInstances(HelixAdmin helixAdmin, String helixClusterName,
      BrokerDataResource brokerDataResource) {
    final Random random = new Random(System.currentTimeMillis());
    int numInstances = brokerDataResource.getNumBrokerInstances();
    String resourceName = brokerDataResource.getResourceName();
    List<String> allInstanceList =
        helixAdmin.getInstancesInClusterWithTag(helixClusterName, brokerDataResource.getTag());
    if (allInstanceList.size() < brokerDataResource.getNumBrokerInstances()) {
      throw new RuntimeException("Current number of broker instances with tag : " + brokerDataResource.getTag()
          + " is less than required number of broker instances : " + brokerDataResource.getNumBrokerInstances());
    }
    List<String> selectedInstanceList = new ArrayList<String>();
    for (int i = 0; i < numInstances; ++i) {
      final int idx = random.nextInt(allInstanceList.size());
      selectedInstanceList.add(allInstanceList.get(idx));
      allInstanceList.remove(idx);
    }
    LOGGER.info("Broker resource assignment result for : " + resourceName + ", selected instances: "
        + Arrays.toString(selectedInstanceList.toArray()));

    return selectedInstanceList;
  }
}
