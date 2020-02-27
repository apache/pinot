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
package org.apache.pinot.broker.broker.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.broker.routing.RoutingManager;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker message handler factory for Helix user-define messages.
 * <p>The following message sub-types are supported:
 * <ul>
 *   <li>Refresh segment message: Refresh the routing properties for a given segment</li>
 * </ul>
 */
public class BrokerUserDefineMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerUserDefineMessageHandlerFactory.class);

  private final RoutingManager _routingManager;

  public BrokerUserDefineMessageHandlerFactory(RoutingManager routingManager) {
    _routingManager = routingManager;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String msgSubType = message.getMsgSubType();
    switch (msgSubType) {
      case SegmentRefreshMessage.REFRESH_SEGMENT_MSG_SUB_TYPE:
        return new RefreshSegmentMessageHandler(new SegmentRefreshMessage(message), context);
      default:
        throw new UnsupportedOperationException("Unsupported user defined message sub type: " + msgSubType);
    }
  }

  @Override
  public String getMessageType() {
    return Message.MessageType.USER_DEFINE_MSG.toString();
  }

  @Override
  public void reset() {
  }

  private class RefreshSegmentMessageHandler extends MessageHandler {
    private final String _tableNameWithType;
    private final String _segmentName;

    public RefreshSegmentMessageHandler(SegmentRefreshMessage segmentRefreshMessage, NotificationContext context) {
      super(segmentRefreshMessage, context);
      _tableNameWithType = segmentRefreshMessage.getTableNameWithType();
      _segmentName = segmentRefreshMessage.getSegmentName();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _routingManager.refreshSegment(_tableNameWithType, _segmentName);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      LOGGER.error("Caught exception while refreshing segment: {} of table: {} (code: {}, type: {})", _segmentName,
          _tableNameWithType, errorCode, errorType, e);
    }
  }
}
