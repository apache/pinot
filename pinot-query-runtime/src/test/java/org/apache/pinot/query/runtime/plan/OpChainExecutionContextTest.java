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
package org.apache.pinot.query.runtime.plan;

import java.util.Map;
import org.apache.pinot.core.instance.context.BrokerContext;
import org.apache.pinot.core.instance.context.ServerContext;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.operator.factory.DefaultQueryOperatorFactoryProvider;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class OpChainExecutionContextTest {
  @Test
  public void testSelectsProviderForMailboxRole() {
    MailboxService mailboxService = mock(MailboxService.class);
    QueryOperatorFactoryProvider brokerProvider = mock(QueryOperatorFactoryProvider.class);
    QueryOperatorFactoryProvider serverProvider = mock(QueryOperatorFactoryProvider.class);
    Object previousBrokerProvider = BrokerContext.getInstance().getQueryOperatorFactoryProvider();
    Object previousServerProvider = ServerContext.getInstance().getQueryOperatorFactoryProvider();

    try {
      BrokerContext.getInstance().setQueryOperatorFactoryProvider(brokerProvider);
      ServerContext.getInstance().setQueryOperatorFactoryProvider(serverProvider);

      when(mailboxService.getInstanceType()).thenReturn(InstanceType.BROKER);
      assertThat(newContext(mailboxService).getQueryOperatorFactoryProvider()).isSameAs(brokerProvider);

      when(mailboxService.getInstanceType()).thenReturn(InstanceType.SERVER);
      assertThat(newContext(mailboxService).getQueryOperatorFactoryProvider()).isSameAs(serverProvider);
    } finally {
      BrokerContext.getInstance().setQueryOperatorFactoryProvider(
          previousBrokerProvider != null ? previousBrokerProvider : DefaultQueryOperatorFactoryProvider.INSTANCE);
      ServerContext.getInstance().setQueryOperatorFactoryProvider(
          previousServerProvider != null ? previousServerProvider : DefaultQueryOperatorFactoryProvider.INSTANCE);
    }
  }

  @Test
  public void testFallsBackWhenRoleProviderHasWrongType() {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getInstanceType()).thenReturn(InstanceType.BROKER);

    assertThat(OpChainExecutionContext.getDefaultQueryOperatorFactoryProvider(
        mailboxService, new Object(), mock(QueryOperatorFactoryProvider.class)))
        .isSameAs(DefaultQueryOperatorFactoryProvider.INSTANCE);
  }

  private static OpChainExecutionContext newContext(MailboxService mailboxService) {
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    StageMetadata stageMetadata = mock(StageMetadata.class);
    when(stageMetadata.getStageId()).thenReturn(1);
    WorkerMetadata workerMetadata = mock(WorkerMetadata.class);
    when(workerMetadata.getWorkerId()).thenReturn(2);
    return new OpChainExecutionContext(mailboxService, 3L, "cid", Long.MAX_VALUE, Long.MAX_VALUE, "broker",
        Map.of(), stageMetadata, workerMetadata, null, false, false);
  }
}
