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
package org.apache.pinot.common.utils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.mockito.InOrder;
import org.testng.annotations.Test;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class FileUtilsTest {

  @Test
  public void testSyncAndCloseAggregatesFailures()
      throws IOException {
    FileChannel channel1 = mock(FileChannel.class);
    FileChannel channel2 = mock(FileChannel.class);
    when(channel1.isOpen()).thenReturn(true);
    when(channel2.isOpen()).thenReturn(true);
    doThrow(new IOException("force failed")).when(channel1).force(true);
    doThrow(new IOException("close failed")).when(channel2).close();

    try {
      FileUtils.syncAndClose(channel1, channel2);
      fail("Expected IOException");
    } catch (IOException e) {
      assertEquals(e.getMessage(), "force failed");
      assertEquals(e.getSuppressed().length, 1);
      assertEquals(e.getSuppressed()[0].getMessage(), "close failed");
    }

    // channel1's force() failed, but its close() must still be attempted (unlike a flush-then-close
    // buffer, a channel is always closed even if the force fails, to avoid leaking it); channel2 must
    // also be attempted despite channel1's failure, so a single bad channel doesn't leak the rest.
    InOrder order = inOrder(channel1, channel2);
    order.verify(channel1).isOpen();
    order.verify(channel1).force(true);
    order.verify(channel1).close();
    order.verify(channel2).isOpen();
    order.verify(channel2).force(true);
    order.verify(channel2).close();
  }

  @Test
  public void testSyncAndCloseSkipsForceOnClosedChannel()
      throws IOException {
    FileChannel channel = mock(FileChannel.class);
    when(channel.isOpen()).thenReturn(false);

    FileUtils.syncAndClose(channel);

    verify(channel, never()).force(true);
    verify(channel).close();
  }

  @Test
  public void testForceMappedBuffers()
      throws IOException {
    Path path = Files.createTempFile("FileUtilsTest", null);
    try (FileChannel channel = FileChannel.open(path, READ, WRITE)) {
      channel.truncate(1024);
      MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
      buffer.put(0, (byte) 1);

      FileUtils.forceMappedBuffers(buffer);
    } finally {
      Files.deleteIfExists(path);
    }
  }
}
