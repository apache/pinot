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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a singleton class in the controller that drives the state machines for segments that are in the
 * committing stage.
 *
 * SegmentCompletionManager has a sub-class that represents the FSM that the segment goes through while
 * executing the segment completion protocol between pinot servers and pinot controller. The protocol is
 * described in SegmentCompletionProtocol.
 */
public class SegmentCompletionManager {
  // TODO Can we log using the segment name in the log message?
  public static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionManager.class);

  private final HelixManager _helixManager;
  // A map that holds the FSM for each segment.
  private final Map<String, SegmentCompletionFSM> _fsmMap = new ConcurrentHashMap<>();
  private final Map<String, Long> _commitTimeMap = new ConcurrentHashMap<>();
  private final PinotLLCRealtimeSegmentManager _segmentManager;
  private final ControllerMetrics _controllerMetrics;
  private final LeadControllerManager _leadControllerManager;
  private final SegmentCompletionConfig _segmentCompletionConfig;


  // Half hour max commit time for all segments
  private static final int MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS = 1800;

  public static int getMaxCommitTimeForAllSegmentsSeconds() {
    return MAX_COMMIT_TIME_FOR_ALL_SEGMENTS_SECONDS;
  }

  // TODO keep some history of past committed segments so that we can avoid looking up PROPERTYSTORE if some server
  //  comes in late.

  public SegmentCompletionManager(HelixManager helixManager, PinotLLCRealtimeSegmentManager segmentManager,
      ControllerMetrics controllerMetrics, LeadControllerManager leadControllerManager,
      int segmentCommitTimeoutSeconds, SegmentCompletionConfig segmentCompletionConfig) {
    _helixManager = helixManager;
    _segmentManager = segmentManager;
    _controllerMetrics = controllerMetrics;
    _leadControllerManager = leadControllerManager;
    SegmentCompletionProtocol.setMaxSegmentCommitTimeMs(
        TimeUnit.MILLISECONDS.convert(segmentCommitTimeoutSeconds, TimeUnit.SECONDS));
    _segmentCompletionConfig = segmentCompletionConfig;

    // Initialize the FSM Factory
    SegmentCompletionFSMFactory.init(_segmentCompletionConfig);
  }

  public String getControllerVipUrl() {
    return _segmentManager.getControllerVipUrl();
  }

  protected long getCurrentTimeMs() {
    return System.currentTimeMillis();
  }


  protected StreamPartitionMsgOffsetFactory getStreamPartitionMsgOffsetFactory(LLCSegmentName llcSegmentName) {
    String rawTableName = llcSegmentName.getTableName();
    TableConfig tableConfig = _segmentManager.getTableConfig(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
    StreamConfig streamConfig = IngestionConfigUtils.getFirstStreamConfig(tableConfig);
    return StreamConsumerFactoryProvider.create(streamConfig).createStreamMsgOffsetFactory();
  }

  public Long getCommitTime(String tableName) {
    return _commitTimeMap.get(tableName);
  }

  public void setCommitTime(String tableName, long commitTime) {
    _commitTimeMap.put(tableName, commitTime);
  }

  public ControllerMetrics getControllerMetrics() {
    return _controllerMetrics;
  }

  private SegmentCompletionFSM lookupOrCreateFsm(LLCSegmentName llcSegmentName, String msgType) {
    return _fsmMap.computeIfAbsent(llcSegmentName.getSegmentName(), k -> createFsm(llcSegmentName, msgType));
  }

  private SegmentCompletionFSM createFsm(LLCSegmentName llcSegmentName, String msgType) {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(llcSegmentName.getTableName());
    String segmentName = llcSegmentName.getSegmentName();
    SegmentZKMetadata segmentMetadata = _segmentManager.getSegmentZKMetadata(realtimeTableName, segmentName, null);
    Preconditions.checkState(segmentMetadata != null, "Failed to find ZK metadata for segment: %s", segmentName);

    TableConfig tableConfig = _segmentManager.getTableConfig(realtimeTableName);
    String factoryName = null;
    try {
      Map<String, String> streamConfigMap = IngestionConfigUtils.getFirstStreamConfigMap(tableConfig);
      factoryName = streamConfigMap.get(StreamConfigProperties.SEGMENT_COMPLETION_FSM_SCHEME);
    } catch (Exception e) {
      // If there is an exception, we default to the default factory.
    }

    if (factoryName == null) {
      factoryName = PauselessConsumptionUtils.isPauselessEnabled(tableConfig)
          ? _segmentCompletionConfig.getDefaultPauselessFsmScheme() : _segmentCompletionConfig.getDefaultFsmScheme();
    }

    Preconditions.checkState(SegmentCompletionFSMFactory.isFactoryTypeSupported(factoryName),
        "No FSM registered for name: " + factoryName);

    SegmentCompletionFSM fsm =
        SegmentCompletionFSMFactory.createFSM(factoryName, this, _segmentManager, llcSegmentName, segmentMetadata);
    fsm.transitionToInitialState(msgType);

    LOGGER.info("Created FSM {}", fsm);
    return fsm;
  }

  /**
   * This method is to be called when a server calls in with the segmentConsumed() API, reporting an offset in the
   * stream
   * that it currently has (i.e. next offset that it will consume, if it continues to consume).
   */
  public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final String stopReason = reqParams.getReason();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());

    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    SegmentCompletionFSM fsm = null;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_CONSUMED);
      response = fsm.segmentConsumed(instanceId, offset, stopReason);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentConsumed for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when a server calls in with the segmentCommit() API. The server sends in the segment
   * along with the API, but it is the caller's responsibility to save the segment after this call (and before the
   * segmentCommitEnd() call).
   *
   * If successful, this method will return Response.COMMIT_CONTINUE, in which case, the caller should save the incoming
   * segment and then call segmentCommitEnd().
   *
   * Otherwise, this method will return a protocol response to be returned to the client right away (without saving the
   * incoming segment).
   */
  public SegmentCompletionProtocol.Response segmentCommitStart(
      final SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.segmentCommitStart(reqParams);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentCommitStart for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  public SegmentCompletionProtocol.Response extendBuildTime(final SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    final int extTimeSec = reqParams.getExtraTimeSec();
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.extendBuildTime(instanceId, offset, extTimeSec);
    } catch (Exception e) {
      LOGGER.error("Caught exception in extendBuildTime for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when a server reports that it has stopped consuming a real-time segment.
   *
   * @return
   */
  public SegmentCompletionProtocol.Response segmentStoppedConsuming(
      SegmentCompletionProtocol.Request.Params reqParams) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    final String instanceId = reqParams.getInstanceId();
    final StreamPartitionMsgOffsetFactory factory = getStreamPartitionMsgOffsetFactory(segmentName);
    final StreamPartitionMsgOffset offset = factory.create(reqParams.getStreamPartitionMsgOffset());
    final String reason = reqParams.getReason();
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_STOPPED_CONSUMING);
      response = fsm.stoppedConsuming(instanceId, offset, reason);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentStoppedConsuming for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }
    return response;
  }

  /**
   * This method is to be called when the segment sent in by the server has been saved locally in the correct path that
   * is downloadable by the servers.
   *
   * It returns a response code to be sent back to the client.
   *
   * If the response code is not COMMIT_SUCCESS, then the caller may remove the segment that has been saved.
   */
  public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams,
      CommittingSegmentDescriptor committingSegmentDescriptor) {
    final String segmentNameStr = reqParams.getSegmentName();
    final LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
    final String tableName = segmentName.getTableName();
    if (!isLeader(tableName) || !_helixManager.isConnected()) {
      return SegmentCompletionProtocol.RESP_NOT_LEADER;
    }
    SegmentCompletionFSM fsm = null;
    SegmentCompletionProtocol.Response response = SegmentCompletionProtocol.RESP_FAILED;
    try {
      fsm = lookupOrCreateFsm(segmentName, SegmentCompletionProtocol.MSG_TYPE_COMMIT);
      response = fsm.segmentCommitEnd(reqParams, committingSegmentDescriptor);
    } catch (Exception e) {
      LOGGER.error("Caught exception in segmentCommitEnd for segment {}", segmentNameStr, e);
    }
    if (fsm != null && fsm.isDone()) {
      LOGGER.info("Removing FSM (if present):{}", fsm.toString());
      _fsmMap.remove(segmentNameStr);
    }

    return response;
  }

  @VisibleForTesting
  protected boolean isLeader(String tableName) {
    return _leadControllerManager.isLeaderForTable(tableName);
  }
}
