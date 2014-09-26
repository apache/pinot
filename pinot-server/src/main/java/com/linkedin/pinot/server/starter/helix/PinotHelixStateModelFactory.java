package com.linkedin.pinot.server.starter.helix;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.starter.ServerInstance;


public class PinotHelixStateModelFactory extends StateModelFactory<StateModel> {

  private final String _helixClusterName;
  private final String _instanceName;
  private PinotStateModel _pinotStateModel;

  @Override
  public StateModel createNewStateModel(String partitionName) {
    try {
      this._pinotStateModel = new PinotStateModel(StringUtils.split(partitionName, "_")[0], _instanceName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return this._pinotStateModel;
  }

  public PinotHelixStateModelFactory(String instanceName, String helixClusterName) {
    this._helixClusterName = helixClusterName;
    this._instanceName = instanceName;
  }

  public void shutdown() throws Exception {
  }

  public boolean isStarted() {
    if (this._pinotStateModel != null) {
      return this._pinotStateModel.isStarted();
    }
    return false;
  }

  public boolean isOnline() {
    return this._pinotStateModel.isOnline();
  }

  @StateModelInfo(states = "{'OFFLINE','ONLINE', 'ERROR', 'DROPPED'}", initialState = "OFFINE")
  public static class PinotStateModel extends StateModel {
    private static final Logger LOGGER = LoggerFactory.getLogger("PinotHelixStarter");

    private final String _resourceName;
    private final String _instanceName;

    private boolean _isStarted = false;
    private boolean _isOnline = false;
    private boolean _isError = false;

    private static ServerConf _serverConf;
    private static ServerInstance _serverInstance;

    public PinotStateModel(String resourceName, String instanceName) throws Exception {
      this._resourceName = resourceName;
      this._instanceName = instanceName;

    }

    private void startServerInstance(String resourceName) throws Exception {
      _serverConf = getInstanceServerConfig(resourceName);
      if (_serverInstance == null) {
        LOGGER.info("Trying to create a new ServerInstance!");
        _serverInstance = new ServerInstance();
        LOGGER.info("Trying to initial ServerInstance!");
        _serverInstance.init(_serverConf);
        LOGGER.info("Trying to start ServerInstance!");
        _serverInstance.start();
      }
    }

    private ServerConf getInstanceServerConfig(String resourceName) {
      // TODO Auto-generated method stub
      return null;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      System.out.println("PinotStateModel.onBecomeOnlineFromOffline()");

      // lets get the latest properties from zk

      HelixDataAccessor accessor = context.getManager().getHelixDataAccessor();
      PropertyKey.Builder builder = accessor.keyBuilder();
      HelixProperty resourceHelixProperty = accessor.getProperty(builder.resourceConfig(_resourceName));
      HelixProperty instanceHelixProperty = accessor.getProperty(builder.instanceConfig(_instanceName));
      // this is where I can start bulding the sensei server now and then start it
      try {

        if (this._isStarted && this._isOnline) {
          LOGGER
              .info("################### looks like the server is already started and sucessfully went online... will skip booting up pinot");
          return;
        }

        this._isOnline = true;
        this._isStarted = true;

      } catch (Exception e) {
        this._isOnline = false;
        this._isStarted = true;
        LOGGER.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage());
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      System.out.println("PinotStateModel.onBecomeOfflineFromOnline()");
      try {
        LOGGER.info("###################### going offline");

        this._isOnline = false;
        this._isStarted = false;
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage());
      }
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {

      LOGGER.info("################## enteterd error state");
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOGGER.info("dropped callback got invoked, this should not get invoked, lid stop and start this instance");
    }

    public boolean isStarted() {
      return _isStarted;
    }

    public boolean isOnline() {
      return _isOnline;
    }

    public boolean isError() {
      return _isError;
    }

  }
}
