/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.tools.query.comparison;

import com.linkedin.pinot.common.request.helper.ControllerRequestBuilder;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;
import com.linkedin.pinot.tools.admin.command.DeleteClusterCommand;
import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import com.linkedin.pinot.tools.admin.command.StartBrokerCommand;
import com.linkedin.pinot.tools.admin.command.StartControllerCommand;
import com.linkedin.pinot.tools.admin.command.StartServerCommand;
import com.linkedin.pinot.tools.admin.command.StartZookeeperCommand;
import com.linkedin.pinot.tools.admin.command.UploadSegmentCommand;
import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.tools.admin.command.AbstractBaseCommand.sendPostRequest;


public class ClusterStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStarter.class);
  private final QueryComparisonConfig _config;
  private String _controllerPort;
  private String _brokerHost;
  private String _brokerPort;
  private String _serverPort;
  private String _zkAddress;
  private String _clusterName;
  private final String _localhost;

  private File _segmentDir;
  private long TIMEOUT_IN_SECONDS = 200 * 1000;

  ClusterStarter(QueryComparisonConfig config)
      throws SocketException, UnknownHostException {
    _config = config;
    _segmentDir = new File(config.getSegmentsDir());
    _localhost = NetUtil.getHostAddress();

    _zkAddress = config.getZookeeperAddress();
    _clusterName = config.getClusterName();

    _controllerPort = config.getControllerPort();
    _brokerHost = config.getBrokerHost();
    _brokerPort = config.getBrokerPort();
    _serverPort = config.getServerPort();
  }

  private void startZookeeper()
      throws IOException {
    if (_config.getStartZookeeper()) {
      StartZookeeperCommand zkStarter = new StartZookeeperCommand();
      zkStarter.execute();
    }
  }

  private void startController()
      throws Exception {

    // Delete existing cluster first.
    DeleteClusterCommand deleteClusterCommand = new DeleteClusterCommand().setClusterName(_clusterName);
    deleteClusterCommand.execute();

    StartControllerCommand controllerStarter =
        new StartControllerCommand().setControllerPort(_controllerPort).setZkAddress(_zkAddress)
            .setClusterName(_clusterName);

    controllerStarter.execute();
  }

  private void startBroker()
      throws Exception {
    StartBrokerCommand brokerStarter =
        new StartBrokerCommand().setClusterName(_clusterName).setPort(Integer.valueOf(_brokerPort));
    brokerStarter.execute();
  }

  private void startServer()
      throws Exception {
    StartServerCommand serverStarter =
        new StartServerCommand().setPort(Integer.valueOf(_serverPort)).setClusterName(_clusterName);
    serverStarter.execute();
  }

  private void addTable()
      throws JSONException, IOException {
    String tableName = _config.getTableName();

    if (tableName == null) {
      LOGGER.error("Table name not specified in configuration");
      return;
    }

    String controllerAddress = "http://" + _localhost + ":" + _controllerPort;
    JSONObject request = ControllerRequestBuilder
        .buildCreateOfflineTableJSON(tableName, "server", "broker", _config.getTimeColumnName(), _config.getTimeUnit(),
            "", "", 3, "BalanceNumSegmentAssignmentStrategy");
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(controllerAddress).forTableCreate(), request.toString());
  }

  private void uploadData()
      throws Exception {
    UploadSegmentCommand segmentUploader =
        new UploadSegmentCommand().setSegmentDir(_segmentDir.getAbsolutePath()).setControllerHost(_localhost)
            .setControllerPort(_controllerPort);
    segmentUploader.execute();
    waitForExternalViewUpdate();
  }

  public void start()
      throws Exception {
    startZookeeper();
    startController();
    startBroker();
    startServer();
    addTable();
    uploadData();
  }

  public String query(String query)
      throws Exception {
    LOGGER.info("Running query on Pinot Cluster");
    PostQueryCommand queryRunner =
        new PostQueryCommand().setQuery(query).setBrokerHost(_brokerHost).setBrokerPort(_brokerPort);
    return queryRunner.run();
  }

  private void waitForExternalViewUpdate() {
    final ZKHelixAdmin helixAdmin = new ZKHelixAdmin(_zkAddress);
    ClusterStateVerifier.Verifier customVerifier = new ClusterStateVerifier.Verifier() {

      @Override
      public boolean verify() {
        List<String> resourcesInCluster = helixAdmin.getResourcesInCluster(_clusterName);
        LOGGER.info("Waiting for external view to update " + new Timestamp(System.currentTimeMillis()));

        for (String resourceName : resourcesInCluster) {
          IdealState idealState = helixAdmin.getResourceIdealState(_clusterName, resourceName);
          ExternalView externalView = helixAdmin.getResourceExternalView(_clusterName, resourceName);

          if (idealState == null || externalView == null) {
            return false;
          }

          Set<String> partitionSet = idealState.getPartitionSet();
          for (String partition : partitionSet) {
            Map<String, String> instanceStateMapIS = idealState.getInstanceStateMap(partition);
            Map<String, String> instanceStateMapEV = externalView.getStateMap(partition);

            if (instanceStateMapIS == null || instanceStateMapEV == null) {
              return false;
            }
            if (!instanceStateMapIS.equals(instanceStateMapEV)) {
              return false;
            }
          }
        }

        LOGGER.info("External View updated successfully.");
        return true;
      }
    };
    ClusterStateVerifier.verifyByPolling(customVerifier, TIMEOUT_IN_SECONDS);
  }
}
