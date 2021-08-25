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
package org.apache.pinot.controller;

import java.util.Properties;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Factory class for creating message handlers for incoming helix messages. */
public class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerUserDefinedMessageHandlerFactory.class);
  private static final String USER_DEFINED_MSG_STRING = Message.MessageType.USER_DEFINE_MSG.toString();

  private final PeriodicTaskScheduler _periodicTaskScheduler;

  public ControllerUserDefinedMessageHandlerFactory(PeriodicTaskScheduler periodicTaskScheduler) {
    _periodicTaskScheduler = periodicTaskScheduler;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext notificationContext) {
    String messageType = message.getMsgSubType();
    if (messageType.equals(RunPeriodicTaskMessage.RUN_PERIODIC_TASK_MSG_SUB_TYPE)) {
      return new RunPeriodicTaskMessageHandler(new RunPeriodicTaskMessage(message), notificationContext,
          _periodicTaskScheduler);
    }

    // Log a warning and return no-op message handler for unsupported message sub-types. This can happen when
    // a new message sub-type is added, and the sender gets deployed first while receiver is still running the
    // old version.
    LOGGER.warn("Received message with unsupported sub-type: {}, using no-op message handler", messageType);
    return new NoOpMessageHandler(message, notificationContext);
  }

  @Override
  public String getMessageType() {
    return USER_DEFINED_MSG_STRING;
  }

  @Override
  public void reset() {
  }

  /** Message handler for {@link RunPeriodicTaskMessage} message. */
  private static class RunPeriodicTaskMessageHandler extends MessageHandler {
    private final String _periodicTaskRequestId;
    private final String _periodicTaskName;
    private final String _tableNameWithType;
    private final PeriodicTaskScheduler _periodicTaskScheduler;

    RunPeriodicTaskMessageHandler(RunPeriodicTaskMessage message, NotificationContext context,
        PeriodicTaskScheduler periodicTaskScheduler) {
      super(message, context);
      _periodicTaskRequestId = message.getPeriodicTaskRequestId();
      _periodicTaskName = message.getPeriodicTaskName();
      _tableNameWithType = message.getTableNameWithType();
      _periodicTaskScheduler = periodicTaskScheduler;
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      LOGGER.info("[TaskRequestId: {}] Handling RunPeriodicTaskMessage by executing task {}", _periodicTaskRequestId,
          _periodicTaskName);
      _periodicTaskScheduler
          .scheduleNow(_periodicTaskName, createTaskProperties(_periodicTaskRequestId, _tableNameWithType));
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      helixTaskResult.setSuccess(true);
      return helixTaskResult;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      LOGGER.error("[TaskRequestId: {}] Message handling error.", _periodicTaskRequestId, e);
    }

    private static Properties createTaskProperties(String periodicTaskRequestId, String tableNameWithType) {
      Properties periodicTaskParameters = new Properties();
      if (periodicTaskRequestId != null) {
        periodicTaskParameters.setProperty(PeriodicTask.PROPERTY_KEY_REQUEST_ID, periodicTaskRequestId);
      }

      if (tableNameWithType != null) {
        periodicTaskParameters.setProperty(PeriodicTask.PROPERTY_KEY_TABLE_NAME, tableNameWithType);
      }

      return periodicTaskParameters;
    }
  }

  /** Message handler for unknown messages */
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
