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
package org.apache.pinot.controller.helix.core.util;

import javax.annotation.Nullable;
import org.apache.helix.ClusterMessagingService;
import org.apache.helix.Criteria;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;


public class MessagingServiceUtils {
  private MessagingServiceUtils() {
  }

  /**
   * Sends a message to the recipients specified by the criteria, returns the messages being sent.
   */
  public static int send(ClusterMessagingService messagingService, Message message, Criteria criteria) {
    try {
      return messagingService.send(criteria, message);
    } catch (Exception e) {
      // NOTE:
      // It can throw exception when the target resource doesn't exist (e.g. ExternalView has not been created yet). It
      // is normal case, and we count it as no message being sent.
      return 0;
    }
  }

  private static int send(ClusterMessagingService messagingService, Message message, String resource,
      @Nullable String partition, @Nullable String instanceName, boolean includingSelf) {
    Criteria criteria = new Criteria();
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setSessionSpecific(true);
    criteria.setResource(resource);
    if (partition != null) {
      criteria.setPartition(partition);
    }
    if (instanceName != null) {
      criteria.setInstanceName(instanceName);
    }
    criteria.setSelfExcluded(!includingSelf);
    return send(messagingService, message, criteria);
  }

  public static int send(ClusterMessagingService messagingService, Message message, String resource,
      @Nullable String partition, @Nullable String instanceName) {
    return send(messagingService, message, resource, partition, instanceName, false);
  }

  public static int send(ClusterMessagingService messagingService, Message message, String resource) {
    return send(messagingService, message, resource, null, null, false);
  }

  public static int sendIncludingSelf(ClusterMessagingService messagingService, Message message, String resource) {
    return send(messagingService, message, resource, null, null, true);
  }
}
