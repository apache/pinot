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
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryWorkloadMessageHandlerFactory implements MessageHandlerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryWorkloadMessageHandlerFactory.class);
    private final ServerMetrics _metrics;

    public QueryWorkloadMessageHandlerFactory(ServerMetrics metrics) {
        _metrics = metrics;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
       String messageType = message.getMsgSubType();
       if (messageType.equals(QueryWorkloadRefreshMessage.REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE)
           || messageType.equals(QueryWorkloadRefreshMessage.DELETE_QUERY_WORKLOAD_MSG_SUB_TYPE)) {
           return new QueryWorkloadRefreshMessageHandler(new QueryWorkloadRefreshMessage(message), context);
        } else {
            throw new IllegalArgumentException("Unknown message subtype: " + messageType);
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

    private static class QueryWorkloadRefreshMessageHandler extends MessageHandler {
        final String _queryWorkloadName;
        final InstanceCost _instanceCost;

        QueryWorkloadRefreshMessageHandler(QueryWorkloadRefreshMessage queryWorkloadRefreshMessage,
                                           NotificationContext context) {
            super(queryWorkloadRefreshMessage, context);
            _queryWorkloadName = queryWorkloadRefreshMessage.getQueryWorkloadName();
            _instanceCost = queryWorkloadRefreshMessage.getInstanceCost();
        }

        @Override
        public HelixTaskResult handleMessage() {
            // TODO: Add logic to invoke the query workload manager to refresh/delete the query workload config
            HelixTaskResult result = new HelixTaskResult();
            result.setSuccess(true);
            return result;
        }

        @Override
        public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
            LOGGER.error("Got error while refreshing query workload config for query workload: {} (error code: {},"
                    + " error type: {})", _queryWorkloadName, errorCode, errorType, e);
        }
    }
}
