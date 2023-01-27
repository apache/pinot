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
package org.apache.pinot.query.runtime.operator.exchange;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BroadcastExchangeTest {
  private static final MailboxIdentifier MAILBOX_1 = new JsonMailboxIdentifier("1", "0@host:1", "0@host:1");
  private static final MailboxIdentifier MAILBOX_2 = new JsonMailboxIdentifier("1", "0@host:1", "0@host:2");

  private AutoCloseable _mocks;

  @Mock
  TransferableBlock _block;
  @Mock
  MailboxService<TransferableBlock> _mailboxService;

  @Mock
  private SendingMailbox<TransferableBlock> _mailbox1;
  @Mock
  private SendingMailbox<TransferableBlock> _mailbox2;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_mailboxService.getSendingMailbox(MAILBOX_1)).thenReturn(_mailbox1);
    Mockito.when(_mailboxService.getSendingMailbox(MAILBOX_2)).thenReturn(_mailbox2);
    Mockito.when(_block.getType()).thenReturn(DataBlock.Type.METADATA);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldBroadcast() {
    // Given:
    ImmutableList<SendingMailbox> destinations = ImmutableList.of(_mailbox1, _mailbox2);

    // When:
    new BroadcastExchange(destinations, TransferableBlockUtils::splitBlock).route(destinations, _block);

    ArgumentCaptor<TransferableBlock> captor = ArgumentCaptor.forClass(TransferableBlock.class);

    Mockito.verify(_mailbox1, Mockito.times(1)).send(captor.capture());
    Assert.assertEquals(captor.getValue(), _block);

    Mockito.verify(_mailbox2, Mockito.times(1)).send(captor.capture());
    Assert.assertEquals(captor.getValue(), _block);
  }
}
