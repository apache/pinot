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
package com.linkedin.pinot.server.starter.helix;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.common.messages.SegmentRefreshMessage;


public class SegmentMessageHandlerFactory implements MessageHandlerFactory {
  private final Logger LOGGER = LoggerFactory.getLogger(SegmentMessageHandlerFactory.class);
  // To serialize the segment refresh calls.
  private final Lock _refreshLock = new ReentrantLock();
  private final SegmentFetcherAndLoader _fetcherAndLoader;

  public SegmentMessageHandlerFactory(SegmentFetcherAndLoader fetcherAndLoader) {
    _fetcherAndLoader = fetcherAndLoader;
  }

  // Called each time a message is received.
  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    try {
      SegmentRefreshMessage refreshMessage = new SegmentRefreshMessage(message);
      return new SegmentRefreshMessageHandler(refreshMessage, context, _refreshLock);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Unrecognized message subtype {}", message.getMsgSubType());
      return null;
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

  public class SegmentRefreshMessageHandler extends MessageHandler {

    private final String _segmentName;
    private final String _tableName;
    private final Lock _refreshLock;

    private final Logger LOGGER;

    public SegmentRefreshMessageHandler(SegmentRefreshMessage refreshMessage, NotificationContext context, Lock refreshLock) {
      super(refreshMessage, context);
      _segmentName = refreshMessage.getPartitionName();
      _tableName = refreshMessage.getResourceName();
      _refreshLock = refreshLock;
      LOGGER = LoggerFactory.getLogger(_tableName + "-" + SegmentRefreshMessageHandler.class);
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      HelixTaskResult result = new HelixTaskResult();
      LOGGER.info("Handling message {}", _message);
      try {
        final long startTime = System.currentTimeMillis();
        LOGGER.info("Waiting for lock to refresh segment {}", _segmentName);
        _refreshLock.lock();
        LOGGER.info("Acquired lock to refresh segment {} (lock-time={}ms)", _segmentName, (System.currentTimeMillis()-startTime));
        // The addOrReplaceOfflineSegment() call can retry multiple times with back-off for loading the same segment.
        // If it does, future segment loads will be stalled on the one segment that we cannot load.
        _fetcherAndLoader.addOrReplaceOfflineSegment(_tableName, _segmentName, /*retryOnFailure=*/false);
        result.setSuccess(true);
      } finally {
        _refreshLock.unlock();
      }
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("onError: {}, {}", type, code, e);
    }
  }
}
