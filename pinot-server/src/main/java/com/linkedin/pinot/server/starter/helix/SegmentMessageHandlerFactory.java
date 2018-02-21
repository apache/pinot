/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.starter.helix;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.messages.SegmentRefreshMessage;
import com.linkedin.pinot.common.messages.SegmentReloadMessage;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMessageHandlerFactory.class);

  private static final List<String> _messageTypes = ImmutableList.of(Message.MessageType.USER_DEFINE_MSG.name());

  // We only allow limited number of segments refresh/reload happen at the same time
  // The reason for that is segment refresh/reload will temporarily use double-sized memory
  private final Semaphore _refreshThreadSemaphore;

  private final SegmentFetcherAndLoader _fetcherAndLoader;
  private final InstanceDataManager _instanceDataManager;

  public SegmentMessageHandlerFactory(SegmentFetcherAndLoader fetcherAndLoader,
      InstanceDataManager instanceDataManager) {
    _fetcherAndLoader = fetcherAndLoader;
    _instanceDataManager = instanceDataManager;
    int maxParallelRefreshThreads = instanceDataManager.getMaxParallelRefreshThreads();
    if (maxParallelRefreshThreads > 0) {
      _refreshThreadSemaphore = new Semaphore(maxParallelRefreshThreads, true);
    } else {
      _refreshThreadSemaphore = null;
    }
  }

  private void acquireSema(String context, Logger logger) throws InterruptedException {
    if (_refreshThreadSemaphore != null) {
      long startTime = System.currentTimeMillis();
      logger.info("Waiting for lock to refresh : {}, queue-length: {}", context,
          _refreshThreadSemaphore.getQueueLength());
      _refreshThreadSemaphore.acquire();
      logger.info("Acquired lock to refresh segment: {} (lock-time={}ms, queue-length={})", context,
          System.currentTimeMillis() - startTime, _refreshThreadSemaphore.getQueueLength());
    } else {
      LOGGER.info("Locking of refresh threads disabled (segment: {})", context);
    }
  }

  private void releaseSema() {
    if (_refreshThreadSemaphore != null) {
      _refreshThreadSemaphore.release();
    }
  }

  // Called each time a message is received.
  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String msgSubType = message.getMsgSubType();
    switch (msgSubType) {
      case SegmentRefreshMessage.REFRESH_SEGMENT_MSG_SUB_TYPE:
        return new SegmentRefreshMessageHandler(new SegmentRefreshMessage(message), context);
      case SegmentReloadMessage.RELOAD_SEGMENT_MSG_SUB_TYPE:
        return new SegmentReloadMessageHandler(new SegmentReloadMessage(message), context);
      default:
        throw new UnsupportedOperationException("Unsupported user defined message sub type: " + msgSubType);
    }
  }

  // Gets called once during start up. We must return the same message type that this factory is registered for.
  @Override
  public String getMessageType() {
    return Message.MessageType.USER_DEFINE_MSG.toString();
  }

  @Override
  public List<String> getMessageTypes() {
    return _messageTypes;
  }

  @Override
  public void reset() {
    LOGGER.info("Reset called");
  }

  private class SegmentRefreshMessageHandler extends MessageHandler {
    private final String _segmentName;
    private final String _tableName;
    private final Logger _logger;

    public SegmentRefreshMessageHandler(SegmentRefreshMessage refreshMessage, NotificationContext context) {
      super(refreshMessage, context);
      _segmentName = refreshMessage.getPartitionName();
      _tableName = refreshMessage.getResourceName();
      _logger = LoggerFactory.getLogger(_tableName + "-" + SegmentRefreshMessageHandler.class);
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      HelixTaskResult result = new HelixTaskResult();
      _logger.info("Handling message: {}", _message);
      try {
        acquireSema(_segmentName, LOGGER);
        // The addOrReplaceOfflineSegment() call can retry multiple times with back-off for loading the same segment.
        // If it does, future segment loads will be stalled on the one segment that we cannot load.
        _fetcherAndLoader.addOrReplaceOfflineSegment(_tableName, _segmentName, /*retryOnFailure=*/false);
        result.setSuccess(true);
      } finally {
        releaseSema();
      }
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      _logger.error("onError: {}, {}", type, code, e);
    }
  }

  private class SegmentReloadMessageHandler extends MessageHandler {
    private final String _segmentName;
    private final String _tableNameWithType;
    private final Logger _logger;

    public SegmentReloadMessageHandler(SegmentReloadMessage segmentReloadMessage, NotificationContext context) {
      super(segmentReloadMessage, context);
      _segmentName = segmentReloadMessage.getPartitionName();
      _tableNameWithType = segmentReloadMessage.getResourceName();
      _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + SegmentReloadMessageHandler.class);
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      _logger.info("Handling message: {}", _message);
      try {
        if (_segmentName.equals("")) {
          acquireSema("ALL", _logger);
          _instanceDataManager.reloadAllSegments(_tableNameWithType);
        } else {
          // Reload one segment
          acquireSema(_segmentName, _logger);
          _instanceDataManager.reloadSegment(_tableNameWithType, _segmentName);
        }
        helixTaskResult.setSuccess(true);
      } catch (Exception e) {
        throw new RuntimeException(
            "Caught exception while reloading segment: " + _segmentName + " in table: " + _tableNameWithType, e);
      } finally {
        releaseSema();
      }
      return helixTaskResult;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      _logger.error("onError: {}, {}", errorType, errorCode, e);
    }
  }
}
