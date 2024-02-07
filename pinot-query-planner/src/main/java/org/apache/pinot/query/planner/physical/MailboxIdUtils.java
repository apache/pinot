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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.query.routing.MailboxMetadata;


public class MailboxIdUtils {
  private MailboxIdUtils() {
  }

  public static final char SEPARATOR = '|';

  public static String toPlanMailboxId(int senderStageId, int senderWorkerId, int receiverStageId,
      int receiverWorkerId) {
    return Integer.toString(senderStageId) + SEPARATOR + senderWorkerId + SEPARATOR + receiverStageId + SEPARATOR
        + receiverWorkerId;
  }

  public static String toMailboxId(long requestId, String planMailboxId) {
    return Long.toString(requestId) + SEPARATOR + planMailboxId;
  }

  public static List<String> toMailboxIds(long requestId, MailboxMetadata mailboxMetadata) {
    return mailboxMetadata.getMailboxIds().stream().map(v -> toMailboxId(requestId, v)).collect(Collectors.toList());
  }

  @VisibleForTesting
  public static String toMailboxId(long requestId, int senderStageId, int senderWorkerId, int receiverStageId,
      int receiverWorkerId) {
    return toMailboxId(requestId, toPlanMailboxId(senderStageId, senderWorkerId, receiverStageId, receiverWorkerId));
  }
}
