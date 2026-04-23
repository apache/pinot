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

import java.util.Collections;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemorySendingMailboxTest {

  @Test
  public void sendDataThrowsWhenQueryTerminated() {
    MailboxService mailboxService = Mockito.mock(MailboxService.class);
    InMemorySendingMailbox mailbox = new InMemorySendingMailbox("test-mailbox", mailboxService, Long.MAX_VALUE,
        new StatMap<>(MailboxSendOperator.StatKey.class));
    RowHeapDataBlock block = new RowHeapDataBlock(Collections.singletonList(new Object[]{"val"}),
        new DataSchema(new String[]{"foo"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));

    try (QueryThreadContext ctx = QueryThreadContext.openForMseTest()) {
      ctx.getExecutionContext().terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "test");

      // Termination check at the top of send(MseBlock.Data) fires before the mailbox service is touched.
      Assert.assertThrows(TerminationException.class, () -> mailbox.send(block));
      Mockito.verifyNoInteractions(mailboxService);
    }
  }
}
