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
package org.apache.pinot.query.planner.physical;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.RoutingInfo;


public class MailboxIdUtils {
  private MailboxIdUtils() {
  }

  public static final char SEPARATOR = '|';

  @VisibleForTesting
  public static String toMailboxId(long requestId, int senderStageId, int senderWorkerId, int receiverStageId,
      int receiverWorkerId) {
    return Long.toString(requestId) + SEPARATOR + senderStageId + SEPARATOR + senderWorkerId + SEPARATOR
        + receiverStageId + SEPARATOR + receiverWorkerId;
  }

  public static List<RoutingInfo> toRoutingInfos(long requestId, int senderStageId, int senderWorkerId,
      int receiverStageId, List<MailboxInfo> receiverMailboxInfos) {
    List<RoutingInfo> routingInfos = new ArrayList<>();
    for (MailboxInfo mailboxInfo : receiverMailboxInfos) {
      String hostname = mailboxInfo.getHostname();
      int port = mailboxInfo.getPort();
      for (int receiverWorkerId : mailboxInfo.getWorkerIds()) {
        routingInfos.add(new RoutingInfo(hostname, port,
            toMailboxId(requestId, senderStageId, senderWorkerId, receiverStageId, receiverWorkerId)));
      }
    }
    return routingInfos;
  }

  public static List<String> toMailboxIds(long requestId, int senderStageId, List<MailboxInfo> senderMailboxInfos,
      int receiverStageId, int receiverWorkerId) {
    List<String> mailboxIds = new ArrayList<>();
    for (MailboxInfo mailboxInfo : senderMailboxInfos) {
      for (int senderWorkerId : mailboxInfo.getWorkerIds()) {
        mailboxIds.add(toMailboxId(requestId, senderStageId, senderWorkerId, receiverStageId, receiverWorkerId));
      }
    }
    return mailboxIds;
  }
}
