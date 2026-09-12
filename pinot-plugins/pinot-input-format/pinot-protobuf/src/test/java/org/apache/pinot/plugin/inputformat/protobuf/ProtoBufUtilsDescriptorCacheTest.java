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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the last-known-good fallback in [ProtoBufUtils#getDescriptor(String, String, boolean)]: the descriptor is
/// fetched fresh on every call (so in-place updates propagate) and the last content that both fetched and resolved
/// successfully is served only when the fetch itself fails. Fetched-but-unresolvable content (corrupt, empty, or
/// missing the requested type) must fail the call without touching the remembered copy, and a slow stale fetch must
/// never roll the remembered copy backward.
public class ProtoBufUtilsDescriptorCacheTest {
  private static final String COUNTING_SCHEME = "counting";

  private File _descriptorFile;
  private byte[] _sampleContent;
  private String _sampleTypeName;
  private byte[] _complexContent;
  private String _complexTypeName;

  /// A "remote" filesystem that serves local files, counts reads, and can simulate network failures, stalled
  /// streams, and copies that leave partial content plus checksum sidecars behind (like Hadoop filesystems do).
  public static class CountingPinotFS extends LocalPinotFS {
    static final AtomicInteger OPEN_CALLS = new AtomicInteger();
    static volatile boolean _failReads = false;
    static volatile CountDownLatch _firstOpenStallLatch = null;
    static final CountDownLatch[] FIRST_OPEN_REACHED_EOF = new CountDownLatch[1];
    static final AtomicReference<File> LAST_COPY_DST = new AtomicReference<>();

    @Override
    public InputStream open(URI uri)
        throws IOException {
      int call = OPEN_CALLS.incrementAndGet();
      if (_failReads) {
        throw new IOException("Simulated network failure: Temporary failure in name resolution");
      }
      InputStream delegate = super.open(uri);
      CountDownLatch stallLatch = _firstOpenStallLatch;
      if (stallLatch != null && call == 1) {
        return stallAtEof(delegate, stallLatch);
      }
      return delegate;
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      LAST_COPY_DST.set(dstFile);
      if (_failReads) {
        // Simulate a Hadoop-style filesystem failing mid-copy after writing partial content and a checksum
        // sidecar into the destination directory
        Files.write(dstFile.toPath(), new byte[]{1, 2, 3});
        Files.write(new File(dstFile.getParentFile(), "." + dstFile.getName() + ".crc").toPath(), new byte[]{4});
        throw new IOException("Simulated network failure: Temporary failure in name resolution");
      }
      super.copyToLocalFile(srcUri, dstFile);
    }

    /// Wraps the stream so that the first reader announces reaching EOF and then blocks until released — used to
    /// hold a stale fetch open while a newer fetch completes.
    private static InputStream stallAtEof(InputStream delegate, CountDownLatch stallLatch) {
      return new FilterInputStream(delegate) {
        @Override
        public int read()
            throws IOException {
          int result = super.read();
          if (result == -1) {
            awaitRelease();
          }
          return result;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
            throws IOException {
          int result = super.read(buffer, offset, length);
          if (result == -1) {
            awaitRelease();
          }
          return result;
        }

        private void awaitRelease()
            throws IOException {
          FIRST_OPEN_REACHED_EOF[0].countDown();
          try {
            stallLatch.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
          }
        }
      };
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    PinotFSFactory.register(COUNTING_SCHEME, CountingPinotFS.class.getName(), null);
    _descriptorFile = File.createTempFile("proto-descriptor-cache-test", ".desc");
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("sample.desc")) {
      _sampleContent = in.readAllBytes();
    }
    try (InputStream in = getClass().getClassLoader().getResourceAsStream("complex_types.desc")) {
      _complexContent = in.readAllBytes();
    }
    _sampleTypeName = firstMessageType(_sampleContent);
    _complexTypeName = firstMessageType(_complexContent);
  }

  @AfterClass
  public void tearDown() {
    _descriptorFile.delete();
  }

  @BeforeMethod
  public void reset()
      throws Exception {
    ProtoBufUtils.clearDescriptorCache();
    CountingPinotFS.OPEN_CALLS.set(0);
    CountingPinotFS._failReads = false;
    CountingPinotFS._firstOpenStallLatch = null;
    CountingPinotFS.FIRST_OPEN_REACHED_EOF[0] = new CountDownLatch(1);
    CountingPinotFS.LAST_COPY_DST.set(null);
    Files.write(_descriptorFile.toPath(), _sampleContent);
  }

  private String remotePath() {
    return COUNTING_SCHEME + "://" + _descriptorFile.getAbsolutePath();
  }

  private static String firstMessageType(byte[] descriptorSetBytes)
      throws Exception {
    return DynamicSchema.parseFrom(descriptorSetBytes).getMessageTypes().iterator().next();
  }

  @Test
  public void testFetchesFreshOnEveryCallAndNeverCopiesToLocal()
      throws Exception {
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);
    assertEquals(CountingPinotFS.OPEN_CALLS.get(), 1);

    // An in-place update of the remote file must be picked up by the next fetch
    Files.write(_descriptorFile.toPath(), _complexContent);
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _complexTypeName);
    assertEquals(CountingPinotFS.OPEN_CALLS.get(), 2);

    // The fetch path streams the content and never goes through the local-copy (temp file) lifecycle
    assertNull(CountingPinotFS.LAST_COPY_DST.get());
  }

  @Test
  public void testFallsBackToLastKnownGoodOnFetchFailure()
      throws Exception {
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);

    CountingPinotFS._failReads = true;
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);
    // The fetch was attempted (and failed) before falling back
    assertEquals(CountingPinotFS.OPEN_CALLS.get(), 2);

    // Recovery: once the network is back, fresh content is fetched and remembered again
    CountingPinotFS._failReads = false;
    Files.write(_descriptorFile.toPath(), _complexContent);
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _complexTypeName);
  }

  @Test
  public void testFetchFailureWithoutFallbackCopyPropagates() {
    CountingPinotFS._failReads = true;
    assertThrows(IOException.class, () -> ProtoBufUtils.getDescriptor(remotePath(), null, true));
  }

  @Test
  public void testFallbackDisabledFailsFastEvenWhenWarm()
      throws Exception {
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);
    CountingPinotFS._failReads = true;
    assertThrows(IOException.class, () -> ProtoBufUtils.getDescriptor(remotePath(), null, false));
  }

  @Test
  public void testUnresolvableFetchedContentFailsAndPreservesFallbackCopy()
      throws Exception {
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);

    // An empty descriptor set parses but resolves no message type: it must fail the call, not serve the fallback
    Files.write(_descriptorFile.toPath(), new byte[0]);
    IllegalStateException emptyFailure =
        expectThrows(IllegalStateException.class, () -> ProtoBufUtils.getDescriptor(remotePath(), null, true));
    assertTrue(emptyFailure.getMessage().contains("no message types"), emptyFailure.getMessage());

    // Unparseable bytes (0xFF = invalid wire type) must fail the same way
    Files.write(_descriptorFile.toPath(), new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    IllegalStateException corruptFailure =
        expectThrows(IllegalStateException.class, () -> ProtoBufUtils.getDescriptor(remotePath(), null, true));
    assertTrue(corruptFailure.getMessage().contains("Invalid protocol buffer descriptor set"),
        corruptFailure.getMessage());

    // A valid set missing the requested message type must also fail without promotion
    Files.write(_descriptorFile.toPath(), _complexContent);
    assertThrows(IllegalStateException.class,
        () -> ProtoBufUtils.getDescriptor(remotePath(), "does.not.Exist", true));

    // None of the failures above may have replaced the last known good copy
    CountingPinotFS._failReads = true;
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _sampleTypeName);
  }

  @Test(timeOut = 60_000)
  public void testStaleConcurrentFetchCannotRollFallbackCopyBackward()
      throws Exception {
    // Thread A fetches the OLD content and stalls at end-of-stream, before publication
    CountDownLatch stallLatch = new CountDownLatch(1);
    CountingPinotFS._firstOpenStallLatch = stallLatch;
    AtomicReference<Throwable> staleFetchFailure = new AtomicReference<>();
    Thread staleFetch = new Thread(() -> {
      try {
        ProtoBufUtils.getDescriptor(remotePath(), null, true);
      } catch (Throwable t) {
        staleFetchFailure.set(t);
      }
    });
    staleFetch.start();
    CountingPinotFS.FIRST_OPEN_REACHED_EOF[0].await();

    // While A is stalled, the remote file is replaced and a newer fetch completes and publishes
    Files.write(_descriptorFile.toPath(), _complexContent);
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _complexTypeName);

    // A resumes and attempts to publish the older content; publication must reject it
    stallLatch.countDown();
    staleFetch.join();
    assertNull(staleFetchFailure.get());

    CountingPinotFS._failReads = true;
    assertEquals(ProtoBufUtils.getDescriptor(remotePath(), null, true).getFullName(), _complexTypeName);
  }

  @Test
  public void testLocalDescriptorReadFreshWithoutFallback()
      throws Exception {
    File localFile = File.createTempFile("proto-descriptor-cache-test-local", ".desc");
    try {
      Files.write(localFile.toPath(), _sampleContent);
      assertEquals(ProtoBufUtils.getDescriptor(localFile.getAbsolutePath(), null, true).getFullName(),
          _sampleTypeName);

      // A local file edited in place must be re-read
      Files.write(localFile.toPath(), _complexContent);
      assertEquals(ProtoBufUtils.getDescriptor(localFile.getAbsolutePath(), null, true).getFullName(),
          _complexTypeName);

      // Local files are never remembered: once the file is gone the call fails even though it succeeded before
      assertTrue(localFile.delete());
      assertThrows(Exception.class, () -> ProtoBufUtils.getDescriptor(localFile.getAbsolutePath(), null, true));
    } finally {
      localFile.delete();
    }
  }

  @Test
  public void testGetFileCopiedToLocalCleansUpTempDirOnCopyFailure() {
    CountingPinotFS._failReads = true;
    assertThrows(IOException.class, () -> ProtoBufUtils.getFileCopiedToLocal(remotePath()));

    // The temp directory must be fully removed even though the failed copy left partial content and a checksum
    // sidecar behind
    File copyDst = CountingPinotFS.LAST_COPY_DST.get();
    assertFalse(copyDst.getParentFile().exists(), "Leaked temp directory: " + copyDst.getParentFile());
  }
}
