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
package org.apache.pinot.common.messages;

import java.util.UUID;
import org.apache.helix.model.Message;
import org.apache.pinot.common.utils.config.QueryWorkloadConfigUtils;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;


/**
 * Message to refresh the query workload on the instances.
 * This message include the host level cost for each instance.
 */
public class QueryWorkloadRefreshMessage extends Message {
  public static final String REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE = "REFRESH_QUERY_WORKLOAD";
  public static final String INSTANCE_COST = "instanceCost";

  /**
   * Constructor for the sender.
   */
  public QueryWorkloadRefreshMessage(String queryWorkloadName, InstanceCost instanceCost) {
    super(Message.MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    setMsgSubType(REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE);
    // Give it infinite time to process the message, as long as session is alive
    setExecutionTimeout(-1);
    QueryWorkloadConfigUtils.updateZNRecordWithInstanceCost(getRecord(), queryWorkloadName, instanceCost);
  }

  /**
   * Constructor for the receiver.
   */
  public QueryWorkloadRefreshMessage(Message message) {
    super(message.getRecord());
    if (!message.getMsgSubType().equals(REFRESH_QUERY_WORKLOAD_MSG_SUB_TYPE)) {
      throw new IllegalArgumentException("Invalid message subtype:" + message.getMsgSubType());
    }
  }

  public String getQueryWorkloadName() {
    return getRecord().getSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME);
  }

  public InstanceCost getInstanceCost() {
    return QueryWorkloadConfigUtils.getInstanceCostFromZNRecord(getRecord());
  }
}
