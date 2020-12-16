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

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.SegmentReloadMessage;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.OfflineSegmentFetcherAndLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMessageHandlerFactory.class);

  private final OfflineSegmentFetcherAndLoader _fetcherAndLoader;
  private final InstanceDataManager _instanceDataManager;
  private final ServerMetrics _metrics;

  public SegmentMessageHandlerFactory(OfflineSegmentFetcherAndLoader fetcherAndLoader, InstanceDataManager instanceDataManager,
      ServerMetrics metrics) {
    _fetcherAndLoader = fetcherAndLoader;
    _instanceDataManager = instanceDataManager;
    _metrics = metrics;
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
        // The number of retry times depends on the retry count in Constants.
        _instanceDataManager.addOrReplaceOfflineSegment(_tableNameWithType, _segmentName);
        result.setSuccess(true);
      } catch (Exception e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.REFRESH_FAILURES, 1);
        Utils.rethrowException(e);
      }
      return result;
    }
  }

  private class SegmentReloadMessageHandler extends DefaultMessageHandler {
    SegmentReloadMessageHandler(SegmentReloadMessage segmentReloadMessage, ServerMetrics metrics,
        NotificationContext context) {
      super(segmentReloadMessage, metrics, context);
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      _logger.info("Handling message: {}", _message);
      try {
        _fetcherAndLoader.reloadSegment(_tableNameWithType, _segmentName);
        helixTaskResult.setSuccess(true);
      } catch (Throwable e) {
        _metrics.addMeteredTableValue(_tableNameWithType, ServerMeter.RELOAD_FAILURES, 1);
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
