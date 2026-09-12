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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotFSFactoryTest {

  @Test
  public void testDefaultPinotFSFactory() {
    PinotFSFactory.init(new PinotConfiguration());
    NoClosePinotFS pinotFS = (NoClosePinotFS) PinotFSFactory.create("file");
    Assert.assertTrue(pinotFS._delegate instanceof LocalPinotFS);
    Assert.assertTrue(PinotFSFactory.isSchemeRegisteredWith("file", LocalPinotFS.class));
    Assert.assertFalse(PinotFSFactory.isSchemeRegisteredWith("file", TestPinotFS.class));
    Assert.assertFalse(PinotFSFactory.isSchemeRegisteredWith("missing", LocalPinotFS.class));

    PinotFS nestedLocalPinotFS = new NoClosePinotFS(new NoClosePinotFS(new LocalPinotFS()));
    Assert.assertTrue(PinotFSFactory.isFileSystemInstanceOf(nestedLocalPinotFS, LocalPinotFS.class));
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

    NoClosePinotFS testPinotFS = (NoClosePinotFS) PinotFSFactory.create("test");
    Assert.assertTrue(testPinotFS._delegate instanceof TestPinotFS);
    Assert.assertEquals(((TestPinotFS) testPinotFS._delegate).getInitCalled(), 1);
    Assert.assertEquals(((TestPinotFS) testPinotFS._delegate).getConfiguration().getProperty("accessKey"), "v1");
    Assert.assertEquals(((TestPinotFS) testPinotFS._delegate).getConfiguration().getProperty("secretKey"), "V2");
    Assert.assertEquals(((TestPinotFS) testPinotFS._delegate).getConfiguration().getProperty("region"), "us-east");

    NoClosePinotFS pinotFS = (NoClosePinotFS) PinotFSFactory.create("file");
    Assert.assertTrue(pinotFS._delegate instanceof LocalPinotFS);
  }

  @Test
  public void testRegisterIfNeededReusesEquivalentFileSystem() {
    String scheme = "registerIfNeededEquivalent";
    PinotConfiguration initialConfiguration = new PinotConfiguration(Map.of("accessKey", "initial"));
    PinotFSFactory.registerIfNeeded(scheme, TestPinotFS.class.getName(), initialConfiguration);

    NoClosePinotFS initialWrapper = (NoClosePinotFS) PinotFSFactory.create(scheme);
    TestPinotFS initialFileSystem = (TestPinotFS) initialWrapper._delegate;
    PinotFSFactory.registerIfNeeded(scheme, TestPinotFS.class.getName(),
        new PinotConfiguration(Map.of("accessKey", "initial")));

    Assert.assertSame(PinotFSFactory.create(scheme), initialWrapper);
    Assert.assertEquals(initialFileSystem.getInitCalled(), 1);
    Assert.assertEquals(initialFileSystem.getConfiguration().getProperty("accessKey"), "initial");
    Assert.assertEquals(initialFileSystem.getCloseCalled(), 0);
  }

  @Test
  public void testRegisterSupportsNullConfiguration() {
    String scheme = "registerNullConfiguration";
    PinotFSFactory.register(scheme, TestPinotFS.class.getName(), null);

    TestPinotFS fileSystem = (TestPinotFS) ((NoClosePinotFS) PinotFSFactory.create(scheme))._delegate;
    Assert.assertEquals(fileSystem.getInitCalled(), 1);
    Assert.assertNull(fileSystem.getConfiguration());
  }

  @Test
  public void testRegisterIfNeededReplacesDifferentFileSystemOrConfiguration() {
    String scheme = "registerIfNeededDifferent";
    PinotFSFactory.register(scheme, LocalPinotFS.class.getName(), new PinotConfiguration());
    PinotFS previousWrapper = PinotFSFactory.create(scheme);

    PinotFSFactory.registerIfNeeded(scheme, TestPinotFS.class.getName(),
        new PinotConfiguration(Map.of("accessKey", "initial")));
    NoClosePinotFS initialWrapper = (NoClosePinotFS) PinotFSFactory.create(scheme);
    TestPinotFS initialFileSystem = (TestPinotFS) initialWrapper._delegate;
    Assert.assertNotSame(initialWrapper, previousWrapper);

    PinotFSFactory.registerIfNeeded(scheme, TestPinotFS.class.getName(),
        new PinotConfiguration(Map.of("accessKey", "replacement")));
    NoClosePinotFS replacementWrapper = (NoClosePinotFS) PinotFSFactory.create(scheme);
    TestPinotFS replacementFileSystem = (TestPinotFS) replacementWrapper._delegate;

    Assert.assertNotSame(replacementWrapper, initialWrapper);
    Assert.assertEquals(replacementFileSystem.getConfiguration().getProperty("accessKey"), "replacement");
    Assert.assertEquals(initialFileSystem.getCloseCalled(), 0);
  }

  @Test
  public void testConcurrentRegisterIfNeededInitializesOnce()
      throws Exception {
    String scheme = "registerIfNeededConcurrent";
    int numThreads = 8;
    CountingPinotFS.reset();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch ready = new CountDownLatch(numThreads);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<PinotFS>> futures = new ArrayList<>(numThreads);
    try {
      for (int i = 0; i < numThreads; i++) {
        futures.add(executor.submit(() -> {
          ready.countDown();
          start.await();
          PinotFSFactory.registerIfNeeded(scheme, CountingPinotFS.class.getName(),
              new PinotConfiguration(Map.of("accessKey", "shared")));
          return PinotFSFactory.create(scheme);
        }));
      }
      Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
      start.countDown();

      PinotFS registeredFileSystem = futures.get(0).get(10, TimeUnit.SECONDS);
      for (Future<PinotFS> future : futures) {
        Assert.assertSame(future.get(10, TimeUnit.SECONDS), registeredFileSystem);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
      Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }

    Assert.assertEquals(CountingPinotFS.CONSTRUCTION_COUNT.get(), 1);
    Assert.assertEquals(CountingPinotFS.INIT_COUNT.get(), 1);
  }

  public static class TestPinotFS extends BasePinotFS {
    public int _initCalled = 0;
    private int _closeCalled;
    private PinotConfiguration _configuration;

    public int getInitCalled() {
      return _initCalled;
    }

    @Override
    public void init(PinotConfiguration configuration) {
      _configuration = configuration;
      _initCalled++;
    }

    public PinotConfiguration getConfiguration() {
      return _configuration;
    }

    public int getCloseCalled() {
      return _closeCalled;
    }

    @Override
    public void close() {
      _closeCalled++;
    }

    @Override
    public boolean mkdir(URI uri) {
      return true;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete)
        throws IOException {
      return true;
    }

    @Override
    public boolean doMove(URI srcUri, URI dstUri)
        throws IOException {
      return true;
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri)
        throws IOException {
      return true;
    }

    @Override
    public boolean exists(URI fileUri)
        throws IOException {
      return true;
    }

    @Override
    public long length(URI fileUri)
        throws IOException {
      return 0L;
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive)
        throws IOException {
      return null;
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
    public boolean isDirectory(URI uri) {
      return false;
    }

    @Override
    public long lastModified(URI uri) {
      return 0L;
    }

    @Override
    public boolean touch(URI uri)
        throws IOException {
      return true;
    }

    @Override
    public InputStream open(URI uri)
        throws IOException {
      return null;
    }
  }

  public static class CountingPinotFS extends TestPinotFS {
    private static final AtomicInteger CONSTRUCTION_COUNT = new AtomicInteger();
    private static final AtomicInteger INIT_COUNT = new AtomicInteger();

    public CountingPinotFS() {
      CONSTRUCTION_COUNT.incrementAndGet();
    }

    private static void reset() {
      CONSTRUCTION_COUNT.set(0);
      INIT_COUNT.set(0);
    }

    @Override
    public void init(PinotConfiguration configuration) {
      INIT_COUNT.incrementAndGet();
      super.init(configuration);
    }
  }
}
