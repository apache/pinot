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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MailboxStatusLogger implements StreamObserver<Mailbox.MailboxStatus> {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxStatusLogger.class);

  private final OpChainExecutionContext _context;
  private final int _receiverStage;

  public MailboxStatusLogger(OpChainExecutionContext context, int receiverStage) {
    _context = context;
    _receiverStage = receiverStage;
  }

  @Override
  public void onNext(Mailbox.MailboxStatus value) {
    try {
      _context.registerOnMDC();
      LOGGER.trace("Received status {} from receiver stage {}", value, _receiverStage);
    } catch (Exception e) {
      _context.unregisterFromMDC();
    }
  }

  @Override
  public void onError(Throwable t) {
    try {
      _context.registerOnMDC();

      if (t instanceof StatusRuntimeException) {
        switch (((StatusRuntimeException) t).getStatus().getCode()) {
          case DEADLINE_EXCEEDED:
            LOGGER.warn("Mailbox from {} to {} timed out on sender side", _context.getStageId(), _receiverStage);
            break;
          case CANCELLED:
            LOGGER.warn("Mailbox from {} to {} cancelled on sender side", _context.getStageId(), _receiverStage);
            break;
          default:
            LOGGER.warn("Mailbox from {} to {} failed with unknown GRPC error on server side",
                _context.getStageId(), _receiverStage, t);
        }
      } else {
        LOGGER.warn("Mailbox from {} to {} failed with unknown error on server side",
            _context.getStageId(), _receiverStage, t);
      }
    } finally {
      _context.unregisterFromMDC();
    }
  }

  @Override
  public void onCompleted() {
    try {
      _context.registerOnMDC();
      LOGGER.trace("Received completion from receiver stage {}", _receiverStage);
    } catch (Exception e) {
      _context.unregisterFromMDC();
    }
  }
}
