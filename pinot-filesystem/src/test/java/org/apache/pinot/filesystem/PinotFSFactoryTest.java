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
package org.apache.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotFSFactoryTest {
  private PinotFSFactory _pinotFSFactory;

  @BeforeMethod
  public void setUp() throws Exception {
    // Use reflection to get a new segment fetcher factory
    Constructor<PinotFSFactory> constructor = PinotFSFactory.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    _pinotFSFactory = constructor.newInstance();
  }

  @Test
  public void testDefaultPinotFSFactory() {
    _pinotFSFactory.init(new PropertiesConfiguration());
    Assert.assertTrue(_pinotFSFactory.create("file") instanceof LocalPinotFS);
  }

  @Test
  public void testCustomizedSegmentFetcherFactory() {
    Configuration config = new PropertiesConfiguration();
    config.addProperty("class.file", LocalPinotFS.class.getName());
    config.addProperty("class.test", TestPinotFS.class.getName());
    PinotFSFactory.init(config);
    PinotFS testPinotFS = PinotFSFactory.create("test");

    Assert.assertTrue(testPinotFS instanceof TestPinotFS);
    Assert.assertEquals(((TestPinotFS) testPinotFS).getInitCalled(), 1);

    Assert.assertTrue(PinotFSFactory.create("file") instanceof LocalPinotFS);
  }

  public static class TestPinotFS extends PinotFS {
    public int initCalled = 0;

    public int getInitCalled() {
      return initCalled;
    }

    @Override
    public void init(Configuration configuration) {
      initCalled++;
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
    public boolean move(URI srcUri, URI dstUri, boolean overwrite) throws IOException {
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
  }
}
