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

import java.util.List;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.pinot.common.messages.RunPeriodicTaskMessage;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Factory class for creating message handlers for incoming helix messages. */
public class ControllerMessageHandlerFactory implements MessageHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerMessageHandlerFactory.class);
  private static final String USER_DEFINED_MSG_STRING = Message.MessageType.USER_DEFINE_MSG.toString();

  private final PeriodicTaskScheduler _periodicTaskScheduler;

  public ControllerMessageHandlerFactory(PeriodicTaskScheduler periodicTaskScheduler) {
    _periodicTaskScheduler = periodicTaskScheduler;
  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext notificationContext) {
    String messageType = message.getMsgSubType();
    if (messageType.equals(RunPeriodicTaskMessage.RUN_PERIODIC_TASK_MSG_SUB_TYPE)) {
      return new RunPeriodicTaskMessageHandler(new RunPeriodicTaskMessage(message), notificationContext, _periodicTaskScheduler);
    }

    LOGGER.warn("Unknown message type {} received by controller. ", messageType);
    return null;
  }

  @Override
  public String getMessageType() {
    return USER_DEFINED_MSG_STRING;
  }

  @Override
  public void reset() {
  }

  /** Message handler for "Run Periodic Task" message. */
  private static class RunPeriodicTaskMessageHandler extends MessageHandler {
    private final String _periodicTaskName;
    private final List<String> _tableNamesWithType;
    private final PeriodicTaskScheduler _periodicTaskScheduler;

    RunPeriodicTaskMessageHandler(RunPeriodicTaskMessage message, NotificationContext context, PeriodicTaskScheduler periodicTaskScheduler) {
      super(message, context);
      _periodicTaskName = message.getPeriodicTaskName();
      _tableNamesWithType = message.getTableNames();
      _periodicTaskScheduler = periodicTaskScheduler;
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      LOGGER.info("Handling RunPeriodicTaskMessage by executing task {}", _periodicTaskName);
      _periodicTaskScheduler.scheduleNow(_periodicTaskName, _tableNamesWithType);
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      helixTaskResult.setSuccess(true);
      return helixTaskResult;
    }

    @Override
    public void onError(Exception e, ErrorCode errorCode, ErrorType errorType) {
      LOGGER.error("Message handling error.", e);
    }
  }
}
