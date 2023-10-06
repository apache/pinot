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

import lombok.AllArgsConstructor;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManager;
import org.apache.pinot.broker.routing.BrokerRoutingManager;
import org.apache.pinot.common.messages.RoutingTableRebuildMessage;
import org.apache.pinot.common.messages.SegmentRefreshMessage;
import org.apache.pinot.common.messages.TableConfigRefreshMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker message handler factory for Helix user-define messages.
 * <p>The following message sub-types are supported:
 * <ul>
 *   <li>Refresh segment message: Refresh the routing properties for a given segment</li>
 * </ul>
 */
@AllArgsConstructor
public class BrokerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerUserDefinedMessageHandlerFactory.class);

  private final BrokerRoutingManager _routingManager;
  private final HelixExternalViewBasedQueryQuotaManager _queryQuotaManager;

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String msgSubType = message.getMsgSubType();
    switch (msgSubType) {
      case SegmentRefreshMessage.REFRESH_SEGMENT_MSG_SUB_TYPE:
        return new RefreshSegmentMessageHandler(new SegmentRefreshMessage(message), context);
      case TableConfigRefreshMessage.REFRESH_TABLE_CONFIG_MSG_SUB_TYPE:
        return new RefreshTableConfigMessageHandler(new TableConfigRefreshMessage(message), context);
      case RoutingTableRebuildMessage.REBUILD_ROUTING_TABLE_MSG_SUB_TYPE:
        return new RebuildRoutingTableMessageHandler(new RoutingTableRebuildMessage(message), context);
      default:
        // NOTE: Log a warning and return no-op message handler for unsupported message sub-types. This can happen when
        //       a new message sub-type is added, and the sender gets deployed first while receiver is still running the
        //       old version.
        LOGGER.warn("Received message with unsupported sub-type: {}, using no-op message handler", msgSubType);
        return new NoOpMessageHandler(message, context);
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
    final String _tableNameWithType;
    final String _segmentName;

    RefreshSegmentMessageHandler(SegmentRefreshMessage segmentRefreshMessage, NotificationContext context) {
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
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing segment: {} of table: {} (error code: {}, error type: {})", _segmentName,
          _tableNameWithType, code, type, e);
    }
  }

  private class RefreshTableConfigMessageHandler extends MessageHandler {
    final String _tableNameWithType;

    RefreshTableConfigMessageHandler(TableConfigRefreshMessage tableConfigRefreshMessage, NotificationContext context) {
      super(tableConfigRefreshMessage, context);
      _tableNameWithType = tableConfigRefreshMessage.getTableNameWithType();
    }

    @Override
    public HelixTaskResult handleMessage() {
      // TODO: Fetch the table config here and pass it into the managers, or consider merging these 2 managers
      _routingManager.buildRouting(_tableNameWithType);
      _queryQuotaManager.initOrUpdateTableQueryQuota(_tableNameWithType);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while refreshing table config for table: {} (error code: {}, error type: {})",
          _tableNameWithType, code, type, e);
    }
  }

  private class RebuildRoutingTableMessageHandler extends MessageHandler {
    final String _tableNameWithType;

    RebuildRoutingTableMessageHandler(RoutingTableRebuildMessage routingTableRebuildMessage,
        NotificationContext context) {
      super(routingTableRebuildMessage, context);
      _tableNameWithType = routingTableRebuildMessage.getTableNameWithType();
    }

    @Override
    public HelixTaskResult handleMessage() {
      _routingManager.buildRouting(_tableNameWithType);
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error while rebuilding routing table for table: {} (error code: {}, error type: {})",
          _tableNameWithType, code, type, e);
    }
  }

  private static class NoOpMessageHandler extends MessageHandler {

    NoOpMessageHandler(Message message, NotificationContext context) {
      super(message, context);
    }

    @Override
    public HelixTaskResult handleMessage() {
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      LOGGER.error("Got error for no-op message handling (error code: {}, error type: {})", code, type, e);
    }
  }
}
