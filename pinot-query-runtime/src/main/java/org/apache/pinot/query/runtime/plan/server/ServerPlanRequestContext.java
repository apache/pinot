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
package org.apache.pinot.query.runtime.plan.server;

import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.plan.PhysicalPlanContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.pipeline.PipelineBreakerResult;
import org.apache.pinot.spi.config.table.TableType;


/**
 * Context class for converting a {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} into
 * {@link PinotQuery} to execute on server.
 */
public class ServerPlanRequestContext extends PhysicalPlanContext {
  protected TableType _tableType;
  protected TimeBoundaryInfo _timeBoundaryInfo;

  protected PinotQuery _pinotQuery;
  protected InstanceRequest _instanceRequest;

  public ServerPlanRequestContext(MailboxService mailboxService, long requestId, int stageId, long timeoutMs,
      long deadlineMs, VirtualServerAddress server, StageMetadata stageMetadata,
      PipelineBreakerResult pipelineBreakerResult, PinotQuery pinotQuery,
      TableType tableType, TimeBoundaryInfo timeBoundaryInfo, boolean traceEnabled) {
    super(mailboxService, requestId, stageId, timeoutMs, deadlineMs, server, stageMetadata, pipelineBreakerResult,
        traceEnabled);
    _pinotQuery = pinotQuery;
    _tableType = tableType;
    _timeBoundaryInfo = timeBoundaryInfo;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public PinotQuery getPinotQuery() {
    return _pinotQuery;
  }

  public void setInstanceRequest(InstanceRequest instanceRequest) {
    _instanceRequest = instanceRequest;
  }

  public InstanceRequest getInstanceRequest() {
    return _instanceRequest;
  }
}
