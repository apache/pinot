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
import com.google.common.collect.Iterators;
import java.util.Iterator;
import org.apache.pinot.query.mailbox.MailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.StringMailboxIdentifier;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HashExchangeTest {

  private static final MailboxIdentifier MAILBOX_1 = new StringMailboxIdentifier("1:host:1:host:1");
  private static final MailboxIdentifier MAILBOX_2 = new StringMailboxIdentifier("1:host:1:host:2");

  private AutoCloseable _mocks;

  @Mock
  TransferableBlock _block;
  @Mock
  MailboxService<TransferableBlock> _mailboxService;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldSplitAndRouteBlocksBasedOnPartitionKey() {
    // Given:
    TestSelector selector = new TestSelector(Iterators.forArray(2, 0, 1));
    Mockito.when(_block.getContainer()).thenReturn(ImmutableList.of(
        new Object[]{0},
        new Object[]{1},
        new Object[]{2}
    ));
    ImmutableList<MailboxIdentifier> destinations = ImmutableList.of(MAILBOX_1, MAILBOX_2);

    // When:
    Iterator<BlockExchange.RoutedBlock> route =
        new HashExchange(_mailboxService, destinations, selector, TransferableBlockUtils::splitBlock)
            .route(destinations, _block);

    // Then:
    BlockExchange.RoutedBlock block1 = route.next();
    Assert.assertEquals(block1._destination, MAILBOX_1);
    Assert.assertEquals(block1._block.getContainer().get(0), new Object[]{0});
    Assert.assertEquals(block1._block.getContainer().get(1), new Object[]{1});

    BlockExchange.RoutedBlock block2 = route.next();
    Assert.assertEquals(block2._destination, MAILBOX_2);
    Assert.assertEquals(block2._block.getContainer().get(0), new Object[]{2});

    Assert.assertFalse(route.hasNext());
  }

  private static class TestSelector implements KeySelector<Object[], Object[]> {

    private final Iterator<Integer> _hashes;

    public TestSelector(Iterator<Integer> hashes) {
      _hashes = hashes;
    }

    @Override
    public Object[] getKey(Object[] input) {
      throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public int computeHash(Object[] input) {
      return _hashes.next();
    }
  }
}
