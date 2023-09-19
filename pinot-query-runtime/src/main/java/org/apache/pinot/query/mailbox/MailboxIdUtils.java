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
package org.apache.pinot.query.mailbox;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.query.routing.MailboxMetadata;


// TODO: De-couple mailbox id from query information
public class MailboxIdUtils {
  private MailboxIdUtils() {
  }

  private static final char SEPARATOR = '|';

  @VisibleForTesting
  public static String toMailboxId(long requestId, int senderStageId, int senderWorkerId, int receiverStageId,
      int receiverWorkerId) {
    return Long.toString(requestId) + SEPARATOR + senderStageId + SEPARATOR + senderWorkerId + SEPARATOR
        + receiverStageId + SEPARATOR + receiverWorkerId;
  }

  public static List<String> toMailboxIds(long requestId, MailboxMetadata mailBoxMetadata) {
    return toMailboxIds(requestId, mailBoxMetadata.getMailBoxIdList());
  }

  public static List<String> toMailboxIds(long requestId, List<String> mailboxMetadataIdList) {
    return mailboxMetadataIdList.stream()
        .map(mailboxIdFromBroker -> Long.toString(requestId) + SEPARATOR + mailboxIdFromBroker)
        .collect(Collectors.toList());
  }
}
