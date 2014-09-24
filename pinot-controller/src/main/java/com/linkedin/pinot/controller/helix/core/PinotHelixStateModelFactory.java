package com.linkedin.pinot.controller.helix.core;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotHelixStateModelFactory extends StateModelFactory<StateModel> {

  private final String helixClusterName;
  private final String instanceName;
  private PinotStateModel pinotStateModel;

  public PinotHelixStateModelFactory(String instanceName, String helixClusterName) {
    this.helixClusterName = helixClusterName;
    this.instanceName = instanceName;
  }

  public void shutdown() throws Exception {
  }

  public boolean isStarted() {
    if (this.pinotStateModel != null) {
      return this.pinotStateModel.isStarted();
    }
    return false;
  }

  public boolean isOnline() {
    return this.pinotStateModel.isOnline();
  }

  @Override
  public StateModel createNewStateModel(String partitionName) {
    this.pinotStateModel = new PinotStateModel(StringUtils.split(partitionName, "_")[0], instanceName);
    return this.pinotStateModel;
  }

  public static class PinotStateModel extends StateModel {
    private static final Logger logger = LoggerFactory.getLogger("PinotHelixStarter");

    private final String resourceName;
    private final String instanceName;

    private boolean isStarted = false;
    private boolean isOnline = false;

    public PinotStateModel(String resourceName, String instanceName) {
      this.resourceName = resourceName;
      this.instanceName = instanceName;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      // lets get the latest properties from zk

      HelixDataAccessor accessor = context.getManager().getHelixDataAccessor();
      PropertyKey.Builder builder = accessor.keyBuilder();
      HelixProperty resourceHelixProperty = accessor.getProperty(builder.resourceConfig(resourceName));
      HelixProperty instanceHelixProperty = accessor.getProperty(builder.instanceConfig(instanceName));
      // this is where I can start bulding the sensei server now and then start it
      try {

        if (this.isStarted && this.isOnline) {
          logger
              .info("################### looks like the server is already started and sucessfully went online... will skip booting up pinot");
          return;
        }

        this.isOnline = true;
        this.isStarted = true;

      } catch (Exception e) {
        this.isOnline = false;
        this.isStarted = true;
        logger.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage());
      }
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      try {
        logger.info("###################### going offline");

        this.isOnline = false;
        this.isStarted = false;
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage());
      }
    }

    public void onBecomeOfflineFromError(Message message, NotificationContext context) {

      logger.info("################## enteterd error state");
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      logger.info("dropped callback got invoked, this should not get invoked, lid stop and start this instance");
    }

    public boolean isStarted() {
      return isStarted;
    }

    public boolean isOnline() {
      return isOnline;
    }

  }
}
