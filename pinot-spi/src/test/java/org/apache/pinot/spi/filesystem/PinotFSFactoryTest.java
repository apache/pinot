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
package org.apache.pinot.spi.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotFSFactoryTest {

  @Test
  public void testDefaultPinotFSFactory() {
    PinotFSFactory.init(new PinotConfiguration());
    Assert.assertTrue(PinotFSFactory.create("file") instanceof LocalPinotFS);
  }

  @Test
  public void testCustomizedSegmentFetcherFactory() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.file", LocalPinotFS.class.getName());

    properties.put("class.test", TestPinotFS.class.getName());
    properties.put("test.accessKey", "v1");
    properties.put("test.secretKey", "V2");
    properties.put("test.region", "us-east");
    PinotFSFactory.init(new PinotConfiguration(properties));

    PinotFS testPinotFS = PinotFSFactory.create("test");
    Assert.assertTrue(testPinotFS instanceof TestPinotFS);
    Assert.assertEquals(((TestPinotFS) testPinotFS).getInitCalled(), 1);
    Assert.assertEquals(((TestPinotFS) testPinotFS).getConfiguration().getProperty("accessKey"), "v1");
    Assert.assertEquals(((TestPinotFS) testPinotFS).getConfiguration().getProperty("secretKey"), "V2");
    Assert.assertEquals(((TestPinotFS) testPinotFS).getConfiguration().getProperty("region"), "us-east");

    Assert.assertTrue(PinotFSFactory.create("file") instanceof LocalPinotFS);
  }

  public static class TestPinotFS extends PinotFS {
    public int initCalled = 0;
    private PinotConfiguration _configuration;

    public int getInitCalled() {
      return initCalled;
    }

    @Override
    public void init(PinotConfiguration configuration) {
      _configuration = configuration;
      initCalled++;
    }

    public PinotConfiguration getConfiguration() {
      return _configuration;
    }

    @Override
    public boolean mkdir(URI uri) {
      return true;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
      return true;
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri) throws IOException {
      return true;
    }

    @Override
    public boolean copy(URI srcUri, URI dstUri) throws IOException {
      return true;
    }

    @Override
    public boolean exists(URI fileUri) throws IOException {
      return true;
    }

    @Override
    public long length(URI fileUri) throws IOException {
      return 0L;
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
      return null;
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    }

    @Override
    public boolean isDirectory(URI uri) {
      return false;
    }

    @Override
    public long lastModified(URI uri) {
      return 0L;
    }

    @Override
    public boolean touch(URI uri) throws IOException {
      return true;
    }

    @Override
    public InputStream open(URI uri) throws IOException {
      return null;
    }
  }
}
