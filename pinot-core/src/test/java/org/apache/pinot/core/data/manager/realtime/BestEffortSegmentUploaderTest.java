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
import java.util.UUID;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BestEffortSegmentUploaderTest {
  private static final int TIMEOUT_IN_MS = 100;
  private File _file;
  @BeforeClass
  public void setUp()
      throws URISyntaxException, IOException, HttpErrorStatusException {
    Configuration fsConfig = new PropertiesConfiguration();
    fsConfig.setProperty("class.hdfs", "org.apache.pinot.core.data.manager.realtime.BestEffortSegmentUploaderTest$AlwaysSucceedPinotFS");
    fsConfig.setProperty("class.timeout", "org.apache.pinot.core.data.manager.realtime.BestEffortSegmentUploaderTest$AlwaysTimeoutPinotFS");
    fsConfig.setProperty("class.existing", "org.apache.pinot.core.data.manager.realtime.BestEffortSegmentUploaderTest$AlwaysExistPinotFS");
    PinotFSFactory.init(fsConfig);
    _file = FileUtils.getFile(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    _file.deleteOnExit();
  }

  @Test
  public void testSuccessfulUpload() {
    SegmentUploader segmentUploader = new BestEffortSegmentUploader("hdfs://root", TIMEOUT_IN_MS, "SERVER:///");
    URI segmentURI = segmentUploader.uploadSegment(_file, "table_REALTIME", "segment1");
    Assert.assertEquals(segmentURI.toString(), StringUtil.join(File.separator,"hdfs://root", "table_REALTIME", "segment1"));
  }

  @Test
  public void testSegmentAlreadyExist() {
    SegmentUploader segmentUploader = new BestEffortSegmentUploader("existing://root", TIMEOUT_IN_MS, "SERVER:///");
    URI segmentURI = segmentUploader.uploadSegment(_file, "table_REALTIME", "segment1");
    Assert.assertEquals(segmentURI.toString(), StringUtil.join(File.separator,"existing://root", "table_REALTIME", "segment1"));
  }

  @Test
  public void testUploadTimeOut() {
    SegmentUploader segmentUploader = new BestEffortSegmentUploader("timeout://root", TIMEOUT_IN_MS, "SERVER:///");
    URI segmentURI = segmentUploader.uploadSegment(_file, "table_REALTIME", "segment1");
    Assert.assertEquals(segmentURI.toString(), "SERVER:///");
  }

  @Test
  public void testNoSegmentStoreConfigured() {
    SegmentUploader segmentUploader = new BestEffortSegmentUploader("", TIMEOUT_IN_MS, "SERVER:///");
    URI segmentURI = segmentUploader.uploadSegment(_file, "table_REALTIME", "segment1");
    Assert.assertEquals(segmentURI.toString(), "SERVER:///");
  }

  public static class AlwaysSucceedPinotFS extends PinotFS {

    @Override
    public void init(Configuration config) {

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
    public boolean copy(URI srcUri, URI dstUri)
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

}

