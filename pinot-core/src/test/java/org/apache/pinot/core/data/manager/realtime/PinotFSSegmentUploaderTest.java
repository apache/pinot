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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.StringUtil;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotFSSegmentUploaderTest {
  private static final int TIMEOUT_IN_MS = 1000;
  private static final String SERVER_A = "Server_host-a_8098";
  private static final String SERVER_B = "Server_host-b_8098";
  private File _file;
  private LLCSegmentName _llcSegmentName;
  private ServerMetrics _serverMetrics = Mockito.mock(ServerMetrics.class);

  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.hdfs",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysSucceedPinotFS");
    properties.put("class.timeout",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysTimeoutPinotFS");
    properties.put("class.existing",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$AlwaysExistPinotFS");
    properties.put("class.record",
        "org.apache.pinot.core.data.manager.realtime.PinotFSSegmentUploaderTest$RecordingPinotFS");
    PinotFSFactory.init(new PinotConfiguration(properties));
    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();
    _llcSegmentName = new LLCSegmentName("test_REALTIME", 1, 0, System.currentTimeMillis());
  }

  @BeforeMethod
  public void resetRecordedUploads() {
    RecordingPinotFS.COPIED_DEST_URIS.clear();
  }

  @Test
  public void testSuccessfulUpload() {
    SegmentUploader segmentUploader =
        new PinotFSSegmentUploader("hdfs://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertEquals(segmentURI.toString(), expectedTempUri("hdfs://root", SERVER_A));
  }

  @Test
  public void testSegmentAlreadyExist() {
    SegmentUploader segmentUploader =
        new PinotFSSegmentUploader("existing://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertEquals(segmentURI.toString(), expectedTempUri("existing://root", SERVER_A));
  }

  @Test
  public void testUploadTimeOut() {
    SegmentUploader segmentUploader =
        new PinotFSSegmentUploader("timeout://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertNull(segmentURI);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsUnsafeInstanceId() {
    new PinotFSSegmentUploader("hdfs://root", TIMEOUT_IN_MS, _serverMetrics, "Server_host/8098");
  }

  @Test
  public void testNoSegmentStoreConfigured() {
    SegmentUploader segmentUploader = new PinotFSSegmentUploader("", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    URI segmentURI = segmentUploader.uploadSegment(_file, _llcSegmentName);
    Assert.assertNull(segmentURI);
  }

  @Test
  public void testSameServerRetryReusesTempKey() {
    SegmentUploader segmentUploader =
        new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    URI first = segmentUploader.uploadSegment(_file, _llcSegmentName);
    URI retry = segmentUploader.uploadSegment(_file, _llcSegmentName);
    String expected = expectedTempUri("record://root", SERVER_A);
    Assert.assertEquals(first.toString(), expected);
    Assert.assertEquals(retry.toString(), expected);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.size(), 2);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.get(0).toString(), expected);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.get(1).toString(), expected);
  }

  @Test
  public void testTwoReplicasDoNotShareTempKey() {
    SegmentUploader replicaA = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_A);
    SegmentUploader replicaB = new PinotFSSegmentUploader("record://root", TIMEOUT_IN_MS, _serverMetrics, SERVER_B);
    URI uriA = replicaA.uploadSegment(_file, _llcSegmentName);
    URI uriB = replicaB.uploadSegment(_file, _llcSegmentName);
    String expectedA = expectedTempUri("record://root", SERVER_A);
    String expectedB = expectedTempUri("record://root", SERVER_B);
    Assert.assertEquals(uriA.toString(), expectedA);
    Assert.assertEquals(uriB.toString(), expectedB);
    Assert.assertNotEquals(uriA, uriB);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.size(), 2);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.get(0).toString(), expectedA);
    Assert.assertEquals(RecordingPinotFS.COPIED_DEST_URIS.get(1).toString(), expectedB);
    Assert.assertFalse(expectedA.contains(SERVER_B));
    Assert.assertFalse(expectedB.contains(SERVER_A));
  }

  private String expectedTempUri(String storeRoot, String instanceId) {
    return StringUtil.join(File.separator, storeRoot, _llcSegmentName.getTableName(),
        SegmentCompletionUtils.generateTmpSegmentFileName(_llcSegmentName.getSegmentName(), instanceId));
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

  public static class AlwaysTimeoutPinotFS extends AlwaysSucceedPinotFS {
    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri)
        throws Exception {
      // Make sure the sleep time > the timeout threshold of uploader.
      Thread.sleep(TIMEOUT_IN_MS * 1000);
    }
  }

  public static class AlwaysExistPinotFS extends AlwaysSucceedPinotFS {
    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      return true;
    }
  }

  public static class RecordingPinotFS extends AlwaysSucceedPinotFS {
    static final List<URI> COPIED_DEST_URIS = new ArrayList<>();

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri) {
      COPIED_DEST_URIS.add(dstUri);
    }
  }
}
