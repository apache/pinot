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
package org.apache.pinot.query.runtime.blocks;

import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests transferred, transient and shared ownership without coupling lifetime logic to an operator base class. */
public class ArrowBlockLifecycleTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
  private ArrowQueryContext _arrowContext;
  private OpChainExecutionContext _executionContext;

  @BeforeMethod
  public void setUp() {
    _arrowContext = new ArrowQueryContext(new RootAllocator(16L * 1024 * 1024));
    _executionContext = mock(OpChainExecutionContext.class);
    when(_executionContext.isArrowEnabled()).thenReturn(true);
  }

  @AfterMethod
  public void tearDown() {
    _arrowContext.close();
  }

  @Test
  public void testLeaseConsumesWithoutRetaining() {
    ArrowBlock input = block();
    try (BlockLease lease = BlockLease.open(_executionContext)) {
      assertSame(lease.consume(input), input);
      assertEquals(input.refCount(), 1);
      assertSame(lease.returnOutput(SuccessMseBlock.INSTANCE), SuccessMseBlock.INSTANCE);
    }
    assertEquals(input.refCount(), 0);
    assertEmpty();
  }

  @Test
  public void testLeaseReleasesOnException() {
    ArrowBlock input = block();
    IllegalStateException failure = new IllegalStateException("injected operator failure");
    assertSame(expectThrows(IllegalStateException.class, () -> {
      try (BlockLease lease = BlockLease.open(_executionContext)) {
        lease.consume(input);
        throw failure;
      }
    }), failure);
    assertEquals(input.refCount(), 0);
    assertEmpty();
  }

  @Test
  public void testLeaseTransfersInputAsOutput() {
    ArrowBlock input = block();
    BlockLease lease = BlockLease.open(_executionContext);
    lease.consume(input);
    assertSame(lease.returnOutput(input), input);
    lease.close();
    lease.close();
    assertEquals(input.refCount(), 1);
    input.release();
    assertEmpty();
  }

  @Test
  public void testLeaseKeepsFreshOutputAndReleasesInput() {
    ArrowBlock input = block();
    ArrowBlock output = block();
    try (BlockLease lease = BlockLease.open(_executionContext)) {
      lease.consume(input);
      assertSame(lease.returnOutput(output), output);
    }
    assertEquals(input.refCount(), 0);
    assertEquals(output.refCount(), 1);
    output.release();
    assertEmpty();
  }

  @Test
  public void testFailedLeaseCloseDoesNotTransferOutput() {
    ArrowBlock invalidInput = block();
    invalidInput.release();
    ArrowBlock output = block();
    BlockLease lease = BlockLease.open(_executionContext);
    lease.consume(invalidInput);
    lease.returnOutput(output);
    expectThrows(IllegalStateException.class, lease::close);
    lease.close();
    assertEquals(output.refCount(), 0);
    assertEmpty();
  }

  @Test
  public void testHeldStateOutlivesTransientScope() {
    HeldBlocks held = new HeldBlocks();
    ArrowBlock state = held.holdTransferred(block());
    ArrowBlock input = block();
    try (BlockLease lease = BlockLease.open(_executionContext)) {
      lease.consume(input);
      assertEquals(held.blocks(), List.of(state));
      assertEquals(state.refCount(), 1);
    }
    assertEquals(input.refCount(), 0);
    assertEquals(state.refCount(), 1);
    held.releaseAll();
    held.releaseAll();
    assertTrue(held.blocks().isEmpty());
    assertEmpty();
  }

  @Test
  public void testHeldBlocksReleasesEveryTransferredReference() {
    HeldBlocks held = new HeldBlocks();
    ArrowBlock shared = block();
    shared.retain();
    held.holdTransferred(shared);
    held.holdTransferred(shared);
    assertEquals(shared.refCount(), 2);
    held.releaseAll();
    assertEquals(shared.refCount(), 0);
    assertEmpty();
  }

  @Test
  public void testHeldBlocksContinuesAfterLifetimeViolation() {
    HeldBlocks held = new HeldBlocks();
    ArrowBlock invalid = block();
    invalid.release();
    ArrowBlock valid = block();
    held.holdTransferred(invalid);
    held.holdTransferred(invalid);
    held.holdTransferred(valid);
    IllegalStateException failure = expectThrows(IllegalStateException.class, held::releaseAll);
    assertEquals(failure.getSuppressed().length, 1);
    assertEquals(valid.refCount(), 0);
    assertTrue(held.blocks().isEmpty());
    held.releaseAll();
    assertEmpty();
  }

  @Test
  public void testClosedHoldersRejectNewOwnership() {
    HeldBlocks held = new HeldBlocks();
    held.releaseAll();
    ArrowBlock block = block();
    expectThrows(IllegalStateException.class, () -> held.holdTransferred(block));
    BlockLease lease = BlockLease.open(_executionContext);
    lease.close();
    expectThrows(IllegalStateException.class, () -> lease.consume(block));
    expectThrows(IllegalStateException.class, () -> lease.returnOutput(block));
    assertEquals(block.refCount(), 1);
    block.release();
    assertEmpty();
  }

  @Test
  public void testDisabledLeaseDoesNotRequestAnAllocator() {
    when(_executionContext.isArrowEnabled()).thenReturn(false);
    expectThrows(IllegalStateException.class, () -> BlockLease.open(_executionContext));
    assertEmpty();
  }

  private ArrowBlock block() {
    return ArrowBlockConverter.toArrowBlock(new RowHeapDataBlock(List.<Object[]>of(new Object[]{7}), SCHEMA),
        _arrowContext);
  }

  private void assertEmpty() {
    assertEquals(_arrowContext.getLiveBlockCount(), 0);
    assertEquals(_arrowContext.getAllocator().getAllocatedMemory(), 0L);
  }
}
