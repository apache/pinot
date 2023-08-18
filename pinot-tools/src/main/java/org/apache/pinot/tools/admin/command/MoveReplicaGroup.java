/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.PinotZKChanger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Command to migrate a subset of replica group from current servers to the provided destination servers.
 * This command is intended to be run multiple times to migrate all the replicas of a table to the destination
 * servers (if intended).
 */
@CommandLine.Command(name = "MoveReplicaGroup", description = "Move complete set of segment replica from source "
                                                              + "servers to tagged servers in cluster",
    mixinStandardHelpOptions = true)
public class MoveReplicaGroup extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(MoveReplicaGroup.class);

  @CommandLine.Option(names = {"-srcHosts", "-s", "--src"}, required = true,
      description = "File with names of source hosts or csv list of hostnames")
  private String _srcHosts;

  @CommandLine.Option(names = {"-destHostsFile", "-d", "--dest"}, required = false,
      description = "File with destination servers list")
  private String _destHostsFile = "";

  @CommandLine.Option(names = {"-tableName", "-t", "-table"}, required = true,
      description = "Table name. Supports only OFFLINE table (type is optional)")
  private String _tableName;

  @CommandLine.Option(names = {"-maxSegmentsToMove", "-m", "--max"}, required = false,
      description = "MaxSegmentsToMove")
  private int _maxSegmentsToMove = Integer.MAX_VALUE;

  @CommandLine.Option(names = {"-zkHost", "--zk", "-z"}, required = true,
      description = "Zookeeper host:port")
  private String _zkHost;

  @CommandLine.Option(names = {"-zkPath", "--cluster", "-c"}, required = true,
      description = "Zookeeper cluster path(Ex: /pinot")
  private String _zkPath;

  @CommandLine.Option(names = {"-exec"}, required = false,
      description = "Execute replica group move. dryRun(default) if not specified")
  private boolean _exec = false;

  private ZKHelixAdmin _helix;
  private PinotZKChanger _zkChanger;

  @Override
  public String getName() {
    return "MoveReplicaGroup";
  }

  public String toString() {
    String retString =
        "MoveReplicaGroup -srcHosts " + _srcHosts + " -tableName " + _tableName + " -zkHost " + _zkHost + " -zkPath "
            + _zkPath + (_exec ? " -exec" : "");
    return retString;
  }

  @Override
  public void cleanup() {
  }

  public boolean execute()
      throws IOException, InterruptedException {
    validateParams();

    _zkChanger = new PinotZKChanger(_zkHost, _zkPath);
    _helix = _zkChanger.getHelixAdmin();

    if (!isExistingTable(_tableName)) {
      LOGGER.error("Table {} does not exist", _tableName);
    }

    // expects returned host names to be instance names (Server_<hostName>_<port>)
    List<String> srcHostsList = readSourceHosts();
    LOGGER.info("Source hosts: {}", srcHostsList);

    String serverTenant = getServerTenantName(_tableName) + "_OFFLINE";
    LOGGER.debug("Using server tenant: {}", serverTenant);
    List<String> destinationServers = readDestinationServers();
    LOGGER.info("Destination servers: {}", destinationServers);
    verifyServerLists(srcHostsList, destinationServers);

    Map<String, Map<String, String>> idealStateMap =
        _helix.getResourceIdealState(_zkPath, _tableName).getRecord().getMapFields();

    System.out.println("Existing idealstate:");
    printIdealState(idealStateMap);

    PriorityQueue<SourceSegments> segmentsToMove = getSegmentsToMoveQueue(idealStateMap, srcHostsList);
    PriorityQueue<ServerInstance> destinationServerQueue = getDestinationServerQueue(idealStateMap, destinationServers);

    Map<String, Map<String, String>> proposedIdealState =
        computeNewIdealState(idealStateMap, segmentsToMove, destinationServerQueue, srcHostsList);
    System.out.println("Proposed idealstate:");
    printIdealState(proposedIdealState);
    printDestinationServerCounts(destinationServerQueue);
    if (!_exec) {
      LOGGER.info("Run with -exec to apply this IdealState");
      System.exit(0);
    }

    applyIdealState(proposedIdealState);
    _zkChanger.waitForStable(_tableName);
    return true;
  }

  private List<String> readSourceHosts()
      throws IOException {
    if (_srcHosts.isEmpty()) {
      LOGGER.error("Source hosts(-s) are required");
      System.exit(1);
    }

    File srcFile = new File(_srcHosts);
    List<String> srcHostsList = null;
    if (srcFile.exists()) {
      srcHostsList = readHostsFromFile(_srcHosts);
      if (srcHostsList.isEmpty()) {
        LOGGER.error("Empty list of servers. Nothing to do");
        // this is not process error but most likely usage error
        // exiting with status 1 so that scripts can catch this
        System.exit(1);
      }
    } else {
      List<String> hosts = Arrays.asList(_srcHosts.split("\\s*,\\s*"));
      srcHostsList = hostNameToInstanceNames(hosts);
    }
    return srcHostsList;
  }

  private void printDestinationServerCounts(PriorityQueue<ServerInstance> destinationServerQueue) {
    System.out.println("Number of segments per server: ");
    for (ServerInstance instance : destinationServerQueue) {
      System.out.println(instance._server + " : " + instance._segments);
    }
  }

  private void printIdealState(Map<String, Map<String, String>> idealState)
      throws JsonProcessingException {
    System.out.println(JsonUtils.objectToPrettyString(idealState));
  }

  private void applyIdealState(final Map<String, Map<String, String>> proposedIdealState) {
    HelixHelper.updateIdealState(_zkChanger.getHelixManager(), _tableName, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState input) {
        Map<String, Map<String, String>> existingMapField = input.getRecord().getMapFields();

        for (Map.Entry<String, Map<String, String>> segmentEntry : proposedIdealState.entrySet()) {
          existingMapField.put(segmentEntry.getKey(), segmentEntry.getValue());
        }
        return input;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 500L, 2.0f));
  }

  private Map<String, Map<String, String>> computeNewIdealState(Map<String, Map<String, String>> idealStateMap,
      PriorityQueue<SourceSegments> segmentsToMove, PriorityQueue<ServerInstance> destinationServers,
      List<String> srcHostsList) {

    Map<String, Map<String, String>> newIdealState = copyIdealState(idealStateMap);

    for (int remapCount = 0; remapCount < _maxSegmentsToMove && !segmentsToMove.isEmpty(); remapCount++) {
      String segment = segmentsToMove.poll()._segment;
      Map<String, String> existingMapping = newIdealState.get(segment);
      String destinationServer = getDestinationServer(destinationServers, existingMapping);
      if (destinationServer == null) {
        throw new RuntimeException("No destination server for segment: " + segment);
      }
      String toRemove = null;

      for (Map.Entry<String, String> existingInstanceEntry : existingMapping.entrySet()) {
        if (srcHostsList.contains(existingInstanceEntry.getKey())) {
          toRemove = existingInstanceEntry.getKey();
          break;
        }
      }

      if (toRemove == null) {
        throw new RuntimeException("Could not find a source host to remove for segment: " + segment);
      }
      existingMapping.remove(toRemove);
      existingMapping.put(destinationServer, "ONLINE");
    }
    return newIdealState;
  }

  private String getDestinationServer(PriorityQueue<ServerInstance> destinationServers,
      Map<String, String> existingSegmentMapping) {
    Preconditions.checkNotNull(destinationServers);
    Preconditions.checkArgument(!destinationServers.isEmpty());

    List<ServerInstance> removedServers = new ArrayList<>();
    String selectedServer = null;
    while (!destinationServers.isEmpty()) {
      ServerInstance si = destinationServers.poll();
      removedServers.add(si);
      if (!existingSegmentMapping.containsKey(si._server)) {
        selectedServer = si._server;
        si._segments++;
        break;
      }
    }
    for (ServerInstance removedServer : removedServers) {
      destinationServers.add(removedServer);
    }
    return selectedServer;
  }

  private Map<String, Map<String, String>> copyIdealState(Map<String, Map<String, String>> idealStateMap) {
    Map<String, Map<String, String>> copy = new HashMap<>(idealStateMap);
    for (Map.Entry<String, Map<String, String>> segmentEntry : idealStateMap.entrySet()) {
      Map<String, String> instanceCopy = new HashMap<>(segmentEntry.getValue().size());
      for (Map.Entry<String, String> instanceEntry : segmentEntry.getValue().entrySet()) {
        instanceCopy.put(instanceEntry.getKey(), instanceEntry.getValue());
      }
      copy.put(segmentEntry.getKey(), instanceCopy);
    }
    return copy;
  }

  private void verifyServerLists(List<String> srcHosts, List<String> taggedServers) {
    for (String srcHost : srcHosts) {
      if (taggedServers.contains(srcHost)) {
        LOGGER.error("Source host: {} is also present in destination list", srcHost);
        LOGGER.error("Refusing to migrate replica group");
        System.exit(1);
      }
    }

    // having disabled source hosts in okay since we are moving segments away from source
    if (hasDisabledInstances("Destination", taggedServers)) {
      LOGGER.error("Destination server list has disabled instances. Retry after correcting input");
      System.exit(1);
    }
  }

  private boolean hasDisabledInstances(String logTag, List<String> instances) {
    boolean hasDisabled = false;
    for (String instance : instances) {
      if (!_helix.getInstanceConfig(_zkPath, instance).getInstanceEnabled()) {
        LOGGER.error("{} instance: {} is disabled", logTag, instance);
        hasDisabled = true;
      }
    }
    return hasDisabled;
  }

  private PriorityQueue<ServerInstance> getDestinationServerQueue(Map<String, Map<String, String>> idealStateMap,
      List<String> destServers) {
    // better to keep map rather than removing elements from heap each time
    Map<String, ServerInstance> serverMap = new HashMap<>(destServers.size());
    for (String ds : destServers) {
      serverMap.put(ds, new ServerInstance(ds, 0));
    }

    // For existing mapping of destination servers in idealstate, update the segment count
    for (Map.Entry<String, Map<String, String>> segmentEntry : idealStateMap.entrySet()) {
      for (Map.Entry<String, String> instanceEntry : segmentEntry.getValue().entrySet()) {
        String server = instanceEntry.getKey();
        ServerInstance instance = serverMap.get(server);
        if (instance != null) {
          instance._segments++;
        }
      }
    }

    PriorityQueue<ServerInstance> destServerQueue =
        new PriorityQueue<>(destServers.size(), new Comparator<ServerInstance>() {
          @Override
          public int compare(ServerInstance o1, ServerInstance o2) {
            return o1._segments < o2._segments ? -1 : 1;
          }
        });
    for (Map.Entry<String, ServerInstance> serverEntry : serverMap.entrySet()) {
      destServerQueue.add(serverEntry.getValue());
    }
    return destServerQueue;
  }

  class SourceSegments {
    SourceSegments(String segment, int replicas) {
      _segment = segment;
      _replicaCount = replicas;
    }

    String _segment;
    int _replicaCount;
  }

  class ServerInstance {
    ServerInstance(String server, int segments) {
      _server = server;
      _segments = segments;
    }

    String _server;
    int _segments;
  }

  // this is a priority queue so that we first move those segments which have highest replicasx
  // on srcHosts. This can happen if previous run of the program limited the number of segments to move
  private PriorityQueue<SourceSegments> getSegmentsToMoveQueue(Map<String, Map<String, String>> idealStateMap,
      List<String> srcHosts) {
    PriorityQueue<SourceSegments> sourceSegments =
        new PriorityQueue<>(idealStateMap.keySet().size(), new Comparator<SourceSegments>() {
          @Override
          public int compare(SourceSegments s1, SourceSegments s2) {
            // arbitrary decision for equals case
            return (s1._replicaCount > s2._replicaCount ? -1 : 1);
          }
        });
    for (Map.Entry<String, Map<String, String>> segmentEntry : idealStateMap.entrySet()) {
      SourceSegments srcSegment = new SourceSegments(segmentEntry.getKey(), 0);
      for (Map.Entry<String, String> instanceEntry : segmentEntry.getValue().entrySet()) {
        if (srcHosts.contains(instanceEntry.getKey())) {
          srcSegment._replicaCount++;
        }
      }
      if (srcSegment._replicaCount > 0) {
        sourceSegments.add(srcSegment);
      }
    }
    return sourceSegments;
  }

  private void validateParams() {
    if (_tableName == null || _tableName.isEmpty()) {
      LOGGER.error("Table name is required and can not be empty");
      System.exit(1);
    }
    if (TableNameBuilder.isRealtimeTableResource(_tableName)) {
      LOGGER.error("This operation is not supported for realtime table. table: {}", _tableName);
      System.exit(1);
    }

    _tableName = TableNameBuilder.OFFLINE.tableNameWithType(_tableName);
    if (_zkHost.isEmpty() || _zkPath.isEmpty()) {
      LOGGER.error("zkHost or zkPath should not be empty");
      System.exit(1);
    }
    if (_zkPath.startsWith("/")) {
      _zkPath = _zkPath.substring(1);
    }
    String[] hostSplits = _zkHost.split("/");
    String[] pathSplits = _zkPath.split("/");

    if (hostSplits.length == 1 || (hostSplits.length == 2 && hostSplits[1].isEmpty())) {
      _zkHost = hostSplits[0] + "/" + pathSplits[0];
      _zkPath = Joiner.on("/").join(Arrays.copyOfRange(pathSplits, 1, pathSplits.length));
    }
    LOGGER.info("Using zkHost: {}, zkPath: {}", _zkHost, _zkPath);
  }

  private String getServerTenantName(String tableName)
      throws IOException {
    return getTableConfig(tableName).getTenantConfig().getServer();
  }

  private TableConfig getTableConfig(String tableName)
      throws IOException {
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathBuilder.propertyStore(_zkPath);
    ZkHelixPropertyStore<ZNRecord> propertyStore = new ZkHelixPropertyStore<>(_zkHost, serializer, path);
    ZNRecord tcZnRecord = propertyStore.get("/CONFIGS/TABLE/" + tableName, null, 0);
    TableConfig tableConfig = TableConfigUtils.fromZNRecord(tcZnRecord);
    LOGGER.debug("Loaded table config");
    return tableConfig;
  }

  private boolean isExistingTable(String tableName) {
    return _helix.getResourcesInCluster(_zkPath).contains(tableName);
  }

  private List<String> readDestinationServers()
      throws IOException {
    if (_destHostsFile.isEmpty()) {
      String serverTenant = getServerTenantName(_tableName) + "_OFFLINE";
      LOGGER.debug("Using server tenant: {}", serverTenant);
      return HelixHelper.getEnabledInstancesWithTag(_zkChanger.getHelixManager(), serverTenant);
    } else {
      return readHostsFromFile(_destHostsFile);
    }
  }

  private List<String> readHostsFromFile(String filename)
      throws IOException {
    List<String> hosts = Files.readAllLines(Paths.get(filename), Charset.defaultCharset());
    return hostNameToInstanceNames(hosts);
  }

  private List<String> hostNameToInstanceNames(List<String> hosts) {
    List<String> srcHosts = new ArrayList<>(hosts.size());
    for (String host : hosts) {
      if (host.isEmpty()) {
        continue;
      }
      String server = host.split("_").length == 1 ? "Server_" + host + "_8001" : host;
      srcHosts.add(server);
    }
    return srcHosts;
  }

  public static void main(String[] args)
      throws Exception {
    MoveReplicaGroup mrg = new MoveReplicaGroup();
    CommandLine commandLine = new CommandLine(mrg);
    try {
      commandLine.execute(args);
    } catch (Exception e) {
      LOGGER.error("Failed to parse/execute with arguments", e);
      System.exit(1);
    }
  }
}
