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
package org.apache.pinot.server.starter.helix;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.messages.ForceCommitMessage;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.messages.TableDeletionMessage;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerQueryPhase;
import org.apache.pinot.common.metrics.ServerTimer;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.util.SegmentRefreshSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMessageHandlerFactory.class);

  // We only allow limited number of segments refresh/reload happen at the same time
  // The reason for that is segment refresh/reload will temporarily use double-sized memory
  private final InstanceDataManager _instanceDataManager;
  private final ServerMetrics _metrics;
  private final SegmentRefreshSemaphore _segmentRefreshSemaphore;

  public SegmentMessageHandlerFactory(InstanceDataManager instanceDataManager, ServerMetrics metrics) {
    _instanceDataManager = instanceDataManager;
    _metrics = metrics;
    _segmentRefreshSemaphore = new SegmentRefreshSemaphore(instanceDataManager.getMaxParallelRefreshThreads(), true);
  }

  // Called each time a message is received.
  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String msgSubType = message.getMsgSubType();
    switch (msgSubType) {
      case SegmentRefreshMessage.REFRESH_SEGMENT_MSG_SUB_TYPE:
        return new SegmentRefreshMessageHandler(new SegmentRefreshMessage(message), _metrics, context);
      case SegmentReloadMessage.RELOAD_SEGMENT_MSG_SUB_TYPE:
        return new SegmentReloadMessageHandler(new SegmentReloadMessage(message), _metrics, context);
      case TableDeletionMessage.DELETE_TABLE_MSG_SUB_TYPE:
        return new TableDeletionMessageHandler(new TableDeletionMessage(message), _metrics, context);
      case ForceCommitMessage.FORCE_COMMIT_MSG_SUB_TYPE:
        return new ForceCommitMessageHandler(new ForceCommitMessage(message), _metrics, context);
      default:
        LOGGER.warn("Unsupported user defined message sub type: {} for segment: {}", msgSubType,
            message.getPartitionName());
        return new DefaultMessageHandler(message, _metrics, context);
    }
  }

  // Gets called once during start up. We must return the same message type that this factory is registered for.
  @Override
  public String getMessageType() {
    return Message.MessageType.USER_DEFINE_MSG.toString();
  }

  @Override
  public void reset() {
    LOGGER.info("Reset called");
  }

  private class SegmentRefreshMessageHandler extends DefaultMessageHandler {
    SegmentRefreshMessageHandler(SegmentRefreshMessage refreshMessage, ServerMetrics metrics,
        NotificationContext context) {
      super(refreshMessage, metrics, context);
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult result = new HelixTaskResult();
      _logger.info("Handling message: {}", _message);
      try {
        _segmentRefreshSemaphore.acquireSema(_segmentName, _logger);
        // The number of retry times depends on the retry count in Constants.
        _instanceDataManager.addOrReplaceSegment(_tableNameWithType, _segmentName);
        result.setSuccess(true);
      } catch (Exception e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.REFRESH_FAILURES, 1);
        Utils.rethrowException(e);
      } finally {
        _segmentRefreshSemaphore.releaseSema();
      }
      return result;
    }
  }

  private class SegmentReloadMessageHandler extends DefaultMessageHandler {
    private final boolean _forceDownload;
    private final List<String> _segmentList;

    SegmentReloadMessageHandler(SegmentReloadMessage segmentReloadMessage, ServerMetrics metrics,
        NotificationContext context) {
      super(segmentReloadMessage, metrics, context);
      _forceDownload = segmentReloadMessage.shouldForceDownload();
      _segmentList = segmentReloadMessage.getSegmentList();
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      _logger.info("Handling message: {}", _message);
      try {
        if (CollectionUtils.isNotEmpty(_segmentList)) {
          _instanceDataManager.reloadSegments(_tableNameWithType, _segmentList, _forceDownload,
              _segmentRefreshSemaphore);
        } else if (StringUtils.isNotEmpty(_segmentName)) {
          // TODO: check _segmentName to be backward compatible. Moving forward, we just need to check the list to
          //       reload one or more segments. If the list or the segment name is empty, all segments are reloaded.
          _segmentRefreshSemaphore.acquireSema(_segmentName, _logger);
          try {
            _instanceDataManager.reloadSegment(_tableNameWithType, _segmentName, _forceDownload);
          } finally {
            _segmentRefreshSemaphore.releaseSema();
          }
        } else {
          // NOTE: the method continues if any segment reload encounters an unhandled exception,
          // and failed segments are logged out in the end. We don't acquire any permit here as they'll be acquired
          // by worked threads later.
          _instanceDataManager.reloadAllSegments(_tableNameWithType, _forceDownload, _segmentRefreshSemaphore);
        }
        helixTaskResult.setSuccess(true);
      } catch (Throwable e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.RELOAD_FAILURES, 1);
        // catch all Errors and Exceptions: if we only catch Exception, Errors go completely unhandled
        // (without any corresponding logs to indicate failure!) in the callable path
        throw new RuntimeException(
            "Caught exception while reloading segment: " + _segmentName + " in table: " + _tableNameWithType, e);
      }
      return helixTaskResult;
    }
  }

  private class TableDeletionMessageHandler extends DefaultMessageHandler {
    TableDeletionMessageHandler(TableDeletionMessage tableDeletionMessage, ServerMetrics metrics,
        NotificationContext context) {
      super(tableDeletionMessage, metrics, context);
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      _logger.info("Handling table deletion message");
      try {
        _instanceDataManager.deleteTable(_tableNameWithType);
        helixTaskResult.setSuccess(true);
      } catch (Exception e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETE_TABLE_FAILURES, 1);
        Utils.rethrowException(e);
      }
      try {
        Arrays.stream(ServerMeter.values())
            .filter(m -> !m.isGlobal())
            .forEach(m -> _metrics.removeTableMeter(_tableNameWithType, m));
        Arrays.stream(ServerGauge.values())
            .filter(g -> !g.isGlobal())
            .forEach(g -> _metrics.removeTableGauge(_tableNameWithType, g));
        Arrays.stream(ServerTimer.values())
            .filter(t -> !t.isGlobal())
            .forEach(t -> _metrics.removeTableTimer(_tableNameWithType, t));
        Arrays.stream(ServerQueryPhase.values())
            .forEach(p -> _metrics.removePhaseTiming(_tableNameWithType, p));
      } catch (Exception e) {
        LOGGER.warn("Error while removing metrics of removed table {}. "
            + "Some metrics may survive until the next restart.", _tableNameWithType);
      }
      return helixTaskResult;
    }
  }

  private class ForceCommitMessageHandler extends DefaultMessageHandler {

    private String _tableName;
    private Set<String> _segmentNames;

    public ForceCommitMessageHandler(ForceCommitMessage forceCommitMessage, ServerMetrics metrics,
        NotificationContext ctx) {
      super(forceCommitMessage, metrics, ctx);
      _tableName = forceCommitMessage.getTableName();
      _segmentNames = forceCommitMessage.getSegmentNames();
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      _logger.info("Handling force commit message for table {} segments {}", _tableName, _segmentNames);
      try {
        _instanceDataManager.forceCommit(_tableName, _segmentNames);
        helixTaskResult.setSuccess(true);
      } catch (Exception e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETE_TABLE_FAILURES, 1);
        Utils.rethrowException(e);
      }
      return helixTaskResult;
    }
  }

  private static class DefaultMessageHandler extends MessageHandler {
    final String _segmentName;
    final String _tableNameWithType;
    final ServerMetrics _metrics;
    final Logger _logger;

    DefaultMessageHandler(Message message, ServerMetrics metrics, NotificationContext context) {
      super(message, context);
      _segmentName = message.getPartitionName();
      _tableNameWithType = message.getResourceName();
      _metrics = metrics;
      _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + this.getClass().getSimpleName());
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      helixTaskResult.setSuccess(true);
      return helixTaskResult;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      _logger.error("onError: {}, {}", errorType, errorCode, e);
    }
  }
}
