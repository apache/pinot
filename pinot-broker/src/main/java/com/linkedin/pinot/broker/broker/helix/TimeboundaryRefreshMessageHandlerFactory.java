/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.broker.helix;

import com.linkedin.pinot.broker.routing.HelixExternalViewBasedRouting;
import com.linkedin.pinot.common.messages.TimeboundaryRefreshMessage;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

// Handle the TimeboundaryRefresh message. The Timeboundary refresh requests are handled asynchronously: i.e., they are
// first put into a request map first. The map dedups requests by their tables thus multiple requests for the same
// table only needs to be executed once. A background thread periodically checks the map and performs refreshing for
// all the tables in the map.
public class TimeboundaryRefreshMessageHandlerFactory implements MessageHandlerFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeboundaryRefreshMessageHandlerFactory.class);
    private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
    // A map to store the unique requests (i.e., the table names) to refresh the TimeBoundaryInfo of a pinot table.
    // Ideally a Hashset will suffice but Java util currently does not have Hashset.
    private static ConcurrentHashMap<String, Boolean> _tablesToRefreshmap = new ConcurrentHashMap<>();
    private boolean shuttingDown;

    /**
     *
     * @param helixExternalViewBasedRouting The underlying Routing object to execute TimeboundaryInfo refreshing.
     * @param sleepTimeInMilliseconds The sleep time for the background thread to execute TimeboundaryInfo refreshing.
     */
    public TimeboundaryRefreshMessageHandlerFactory(HelixExternalViewBasedRouting helixExternalViewBasedRouting,
                                                    long sleepTimeInMilliseconds) {
        _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
        // Start a background thread to execute the TimeboundaryInfo update requests.
        Thread tbiUpdateThread = new Thread(new TimeboundaryRefreshMessageExecutor(sleepTimeInMilliseconds));
        tbiUpdateThread.start();
        shuttingDown = false;
    }

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
        String msgSubType = message.getMsgSubType();
        switch (msgSubType) {
            case TimeboundaryRefreshMessage.REFRESH_TIME_BOUNDARY_MSG_SUB_TYPE:
                LOGGER.info("time refresh msg received {} for table {}", message.getPartitionName());
                return new TimeboundaryRefreshMessageHandler(new TimeboundaryRefreshMessage(message), context);
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
        LOGGER.info("Reset called");
    }

    public void shutdown() {
        shuttingDown = true;
    }

    private class TimeboundaryRefreshMessageHandler extends MessageHandler{
        private final String _tableNameWithType;
        private final Logger _logger;


        public TimeboundaryRefreshMessageHandler(TimeboundaryRefreshMessage message, NotificationContext context) {
            super(message, context);
            // The partition name field stores the table name.
            _tableNameWithType = message.getPartitionName();
            _logger = LoggerFactory.getLogger(_tableNameWithType + "-" + TimeboundaryRefreshMessageHandler.class);
        }

        @Override
        public HelixTaskResult handleMessage() {
            HelixTaskResult result = new HelixTaskResult();
            // Put the segment refresh request to a request queue instead of executing immediately. This will reduce the
            // burst of requests when a large number of segments are updated in a short time span.
            _tablesToRefreshmap.put(_tableNameWithType, Boolean.TRUE);
            result.setSuccess(true);
            return result;
        }

        @Override
        public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
            _logger.error("onError: {}, {}", errorType, errorCode, e);
        }
    }
    private class TimeboundaryRefreshMessageExecutor implements Runnable {
        private long _sleepTimeInMilliseconds;
        private final Logger _logger = LoggerFactory.getLogger(TimeboundaryRefreshMessageExecutor.class);;
        public TimeboundaryRefreshMessageExecutor(long sleepTimeInMilliseconds) {
            _sleepTimeInMilliseconds = sleepTimeInMilliseconds;
        }
        @Override
        public void run() {
            while(!shuttingDown) {
                try {
                    ConcurrentHashMap.KeySetView<String, Boolean> tables = _tablesToRefreshmap.keySet();
                    Iterator<String> tableItr = tables.iterator();
                    while(tableItr.hasNext()) {
                        String table = tableItr.next();
                        _logger.info("Update time boundary info for table {} ", table);
                        _helixExternalViewBasedRouting.updateTimeBoundary(table);
                        // Remove the table name from the underlying hashmap.
                        tableItr.remove();
                    }
                    Thread.sleep(_sleepTimeInMilliseconds);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // It is OK for us to break out the loop early because the TimeboundInfo refresh is best effort.
                    break;
                }
            }
            _logger.info("TimeboundaryRefreshMessageExecutor thread has been shutdown.");
        }

    }
}
