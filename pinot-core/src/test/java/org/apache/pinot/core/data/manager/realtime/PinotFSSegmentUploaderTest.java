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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploader.AttemptState;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class PinotFSSegmentUploaderTest {
  private static final int TIMEOUT_IN_MS = 1000;
  private File _file;
  private LLCSegmentName _llcSegmentName;
  private ServerMetrics _serverMetrics = mock(ServerMetrics.class);

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.hdfs",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysSucceedPinotFS");
    properties.put("class.record",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$RecordingPinotFS");
    PinotFSFactory.init(new PinotConfiguration(properties));
    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();
    _llcSegmentName = new LLCSegmentName("test_REALTIME", 1, 0, System.currentTimeMillis());
  }

  @BeforeMethod
  public void resetRecordedUploads() {
    RecordingPinotFS.reset();
    clearInvocations(_serverMetrics);
  }

  @Test
  public void testSuccessfulUpload() {
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics);
    URI first = segmentUploader.uploadSegment(_file, _llcSegmentName);
    URI second = segmentUploader.uploadSegment(_file, _llcSegmentName);
    assertNotNull(first);
    assertNotNull(second);
    assertNotEquals(first, second);
    assertUuidTempUri(first);
    assertUuidTempUri(second);
    assertEquals(RecordingPinotFS.COPIED_DEST_URIS.size(), 2);
    assertTrue(RecordingPinotFS.DELETED_DEST_URIS.isEmpty());
  }

  @Test
  public void testCopyFailureDeletesThatUri() {
    RecordingPinotFS._throwOnCopy = true;
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    assertNull(segmentURI);
    assertEquals(RecordingPinotFS.COPIED_DEST_URIS.size(), 1);
    assertEquals(RecordingPinotFS.DELETED_DEST_URIS, RecordingPinotFS.COPIED_DEST_URIS);
  }

  @Test
  public void testCopyFailureSurvivesThrowingDelete() {
    RecordingPinotFS._throwOnCopy = true;
    RecordingPinotFS._throwOnDelete = true;
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics);
    assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName));
    assertEquals(RecordingPinotFS.COPIED_DEST_URIS.size(), 1);
    assertTrue(RecordingPinotFS.DELETED_DEST_URIS.isEmpty());
  }

  @Test
  public void testTimeoutDeletesOnlyTheAbandonedAttempt()
      throws Exception {
    RecordingPinotFS._enteredCopy = new CountDownLatch(1);
    RecordingPinotFS._releaseCopy = new CountDownLatch(1);
    RecordingPinotFS._deleted = new CountDownLatch(1);
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<URI> first = pool.submit(() -> segmentUploader.uploadSegment(_file, _llcSegmentName, 200));
      assertTrue(RecordingPinotFS._enteredCopy.await(5, TimeUnit.SECONDS));
      assertNull(first.get(5, TimeUnit.SECONDS));
      assertTrue(RecordingPinotFS.DELETED_DEST_URIS.isEmpty());
      URI abandoned = RecordingPinotFS.COPIED_DEST_URIS.get(0);
      RecordingPinotFS._releaseCopy.countDown();
      assertTrue(RecordingPinotFS._deleted.await(5, TimeUnit.SECONDS));
      assertTrue(RecordingPinotFS.DELETED_DEST_URIS.contains(abandoned));

      URI retry = segmentUploader.uploadSegment(_file, _llcSegmentName, TIMEOUT_IN_MS);
      assertNotNull(retry);
      assertNotEquals(retry, abandoned);
      assertFalse(RecordingPinotFS.DELETED_DEST_URIS.contains(retry));
    } finally {
      RecordingPinotFS.releaseCopy();
      pool.shutdownNow();
    }
  }

  @Test
  public void testOverlappingUploadsKeepDistinctUris()
      throws Exception {
    RecordingPinotFS._enteredCopy = new CountDownLatch(2);
    RecordingPinotFS._releaseCopy = new CountDownLatch(1);
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", 30_000, _serverMetrics);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<URI> first = pool.submit(() -> segmentUploader.uploadSegment(_file, _llcSegmentName));
      Future<URI> second = pool.submit(() -> segmentUploader.uploadSegment(_file, _llcSegmentName));
      assertTrue(RecordingPinotFS._enteredCopy.await(5, TimeUnit.SECONDS));
      RecordingPinotFS._releaseCopy.countDown();
      URI uri1 = first.get(5, TimeUnit.SECONDS);
      URI uri2 = second.get(5, TimeUnit.SECONDS);
      assertNotNull(uri1);
      assertNotNull(uri2);
      assertNotEquals(uri1, uri2);
      assertTrue(RecordingPinotFS.DELETED_DEST_URIS.isEmpty());
    } finally {
      RecordingPinotFS.releaseCopy();
      pool.shutdownNow();
    }
  }

  @Test
  public void testAbandonUnlessPublished() {
    AtomicReference<AttemptState> state = new AtomicReference<>(AttemptState.IN_FLIGHT);
    assertFalse(PinotFSSegmentUploader.abandonUnlessPublished(state));
    assertEquals(state.get(), AttemptState.ABANDONED);

    // The worker published first, so the caller owns the URI.
    state.set(AttemptState.PUBLISHED);
    assertTrue(PinotFSSegmentUploader.abandonUnlessPublished(state));
    assertEquals(state.get(), AttemptState.PUBLISHED);

    state.set(AttemptState.FAILED);
    assertFalse(PinotFSSegmentUploader.abandonUnlessPublished(state));
    assertEquals(state.get(), AttemptState.FAILED);
  }

  @Test
  public void testInterruptWhileCopyDoesNotDeleteUntilCopyFinishes()
      throws Exception {
    RecordingPinotFS._enteredCopy = new CountDownLatch(1);
    RecordingPinotFS._releaseCopy = new CountDownLatch(1);
    RecordingPinotFS._deleted = new CountDownLatch(1);
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root", 30_000, _serverMetrics);
    AtomicReference<URI> result = new AtomicReference<>(URI.create("record://sentinel"));
    AtomicBoolean interrupted = new AtomicBoolean();
    Thread caller = new Thread(() -> {
      result.set(segmentUploader.uploadSegment(_file, _llcSegmentName));
      interrupted.set(Thread.currentThread().isInterrupted());
    });
    try {
      caller.start();
      assertTrue(RecordingPinotFS._enteredCopy.await(5, TimeUnit.SECONDS));
      caller.interrupt();
      caller.join(5_000);
      assertFalse(caller.isAlive());
      assertTrue(interrupted.get());
      assertNull(result.get());
      assertTrue(RecordingPinotFS.DELETED_DEST_URIS.isEmpty());
      URI abandoned = RecordingPinotFS.COPIED_DEST_URIS.get(0);
      RecordingPinotFS._releaseCopy.countDown();
      assertTrue(RecordingPinotFS._deleted.await(5, TimeUnit.SECONDS));
      assertEquals(RecordingPinotFS.DELETED_DEST_URIS, List.of(abandoned));
      verify(_serverMetrics).addMeteredTableValue(anyString(),
          eq(ServerMeter.SEGMENT_UPLOAD_FAILURE), eq(1L));
      verify(_serverMetrics, never()).addMeteredTableValue(anyString(),
          eq(ServerMeter.SEGMENT_UPLOAD_SUCCESS), anyLong());
    } finally {
      RecordingPinotFS.releaseCopy();
      caller.interrupt();
      caller.join(5_000);
    }
  }

  @Test
  public void testNoSegmentStoreConfigured() {
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("", TIMEOUT_IN_MS, _serverMetrics);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    assertNull(segmentURI);
  }

  @Test
  public void testInvalidSegmentStoreUriFails() {
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("record://root dir", TIMEOUT_IN_MS, _serverMetrics);
    assertNull(segmentUploader.uploadSegment(_file, _llcSegmentName));
    assertTrue(RecordingPinotFS.COPIED_DEST_URIS.isEmpty());
    verify(_serverMetrics).addMeteredTableValue(anyString(), eq(ServerMeter.SEGMENT_UPLOAD_FAILURE), eq(1L));
  }

  private void assertUuidTempUri(URI segmentURI) {
    String prefix = StringUtil.join(File.separator, "record://root", _llcSegmentName.getTableName(),
        _llcSegmentName.getSegmentName() + ".tmp.");
    assertTrue(segmentURI.toString().startsWith(prefix), segmentURI.toString());
    UUID.fromString(segmentURI.toString().substring(prefix.length()));
  }

  public static class AlwaysSucceedPinotFS extends BasePinotFS {

    @Override
    public void init(PinotConfiguration config) {
    }

    @Override
    public boolean mkdir(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      return false;
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri)
        throws IOException {
      return false;
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri)
        throws IOException {
      return false;
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      return false;
    }

    @Override
    public long length(URI fileUri)
        throws IOException {
      return 0;
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      return new String[0];
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
    }

    @Override
    public boolean isDirectory(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public long lastModified(URI uri)
        throws IOException {
      return 0;
    }

    @Override
    public boolean touch(URI uri)
        throws IOException {
      return false;
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      return null;
    }
  }

  public static class RecordingPinotFS extends AlwaysSucceedPinotFS {
    static final List<URI> COPIED_DEST_URIS = new CopyOnWriteArrayList<>();
    static final List<URI> DELETED_DEST_URIS = new CopyOnWriteArrayList<>();
    static volatile CountDownLatch _enteredCopy;
    static volatile CountDownLatch _releaseCopy;
    static volatile CountDownLatch _deleted;
    static volatile boolean _throwOnCopy;
    static volatile boolean _throwOnDelete;

    static void reset() {
      COPIED_DEST_URIS.clear();
      DELETED_DEST_URIS.clear();
      _throwOnCopy = false;
      _throwOnDelete = false;
      _enteredCopy = null;
      _deleted = null;
      releaseCopy();
    }

    static void releaseCopy() {
      CountDownLatch release = _releaseCopy;
      _releaseCopy = null;
      if (release != null) {
        while (release.getCount() > 0) {
          release.countDown();
        }
      }
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      if (_throwOnDelete) {
        throw new IOException("delete failed");
      }
      if (Thread.currentThread().isInterrupted()) {
        throw new IOException("delete aborted because the thread is interrupted");
      }
      DELETED_DEST_URIS.add(segmentUri);
      CountDownLatch deleted = _deleted;
      if (deleted != null) {
        deleted.countDown();
      }
      return true;
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      COPIED_DEST_URIS.add(dstUri);
      CountDownLatch entered = _enteredCopy;
      if (entered != null) {
        entered.countDown();
      }
      CountDownLatch release = _releaseCopy;
      if (release != null) {
        release.await();
      }
      if (_throwOnCopy) {
        throw new IOException("stored object then failed");
      }
    }
  }
}
