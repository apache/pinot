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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.pinot.spi.crypt.PinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.HTTPS_PROTOCOL;
import static org.apache.pinot.spi.utils.CommonConstants.HTTP_PROTOCOL;
import static org.testng.Assert.assertEquals;


public class SegmentFetcherFactoryTest {
  private static final String FILE_PROTOCOL = "file";
  private static final String TEST_PROTOCOL = "test";
  private static final String TEST_URI = "test://foo/bar";

  @Test
  public void testDefaultSegmentFetcherFactory() {
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTP_PROTOCOL).getClass(), HttpSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTPS_PROTOCOL).getClass(), HttpSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(FILE_PROTOCOL).getClass(), PinotFSSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher("foo").getClass(), PinotFSSegmentFetcher.class);
  }

  @Test(dependsOnMethods = "testDefaultSegmentFetcherFactory")
  public void testCustomizedSegmentFetcherFactory()
      throws Exception {
    Map<String, Object> properties = new HashMap<>();
    properties.put("foo", "bar");
    properties.put("protocols", Arrays.asList(HTTP_PROTOCOL, HTTPS_PROTOCOL, TEST_PROTOCOL, "foo"));
    properties.put("http.foo", "bar");
    properties.put(TEST_PROTOCOL + SegmentFetcherFactory.SEGMENT_FETCHER_CLASS_KEY_SUFFIX,
        FakeSegmentFetcher.class.getName());
    SegmentFetcherFactory.init(new PinotConfiguration(properties));

    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTP_PROTOCOL).getClass(), HttpSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTPS_PROTOCOL).getClass(), HttpsSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(FILE_PROTOCOL).getClass(), PinotFSSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher("foo").getClass(), PinotFSSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(TEST_PROTOCOL).getClass(), FakeSegmentFetcher.class);

    FakeSegmentFetcher testFileFetcher = (FakeSegmentFetcher) SegmentFetcherFactory.getSegmentFetcher(TEST_PROTOCOL);
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 0);

    SegmentFetcherFactory.fetchSegmentToLocal(new URI(TEST_URI), new File("foo/bar"));
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 1);

    SegmentFetcherFactory.fetchSegmentToLocal(TEST_URI, new File("foo/bar"));
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 2);

    // setup crypter
    properties.put("class.fakePinotCrypter", FakePinotCrypter.class.getName());
    PinotCrypterFactory.init(new PinotConfiguration(properties));
    FakePinotCrypter fakeCrypter = (FakePinotCrypter) PinotCrypterFactory.create("fakePinotCrypter");

    SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(TEST_URI, new File("foo/bar"), null);
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 3);
    assertEquals(fakeCrypter._initCalled, 1);
    assertEquals(fakeCrypter._decryptCalled, 0);
    assertEquals(fakeCrypter._encryptCalled, 0);

    SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(TEST_URI, new File("foo/bar"), "fakePinotCrypter");
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 4);
    assertEquals(fakeCrypter._initCalled, 1);
    assertEquals(fakeCrypter._decryptCalled, 1);
    assertEquals(fakeCrypter._encryptCalled, 0);
    assertEquals(fakeCrypter._originalPath, "foo/bar.enc");
    assertEquals(fakeCrypter._decryptedPath, "foo/bar");
  }

  public static class FakeSegmentFetcher implements SegmentFetcher {
    private int _initCalled = 0;
    private int _fetchFileToLocalCalled = 0;

    @Override
    public void init(PinotConfiguration config) {
      _initCalled++;
    }

    @Override
    public void fetchSegmentToLocal(URI uri, File dest)
        throws Exception {
      assertEquals(uri, new URI(TEST_URI));
      _fetchFileToLocalCalled++;
    }

    @Override
    public File fetchUntarSegmentToLocalStreamed(URI uri, File dest, long rateLimit, AtomicInteger attempts)
        throws Exception {
      assertEquals(uri, new URI(TEST_URI));
      _fetchFileToLocalCalled++;
      attempts.set(0);
      return new File("fakeSegmentIndexFile");
    }

    @Override
    public void fetchSegmentToLocal(List<URI> uri, File dest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean fetchSegmentToLocal(String segmentName, Supplier<List<URI>> uriSupplier, File dest) {
      return false;
    }
  }

  public static class FakePinotCrypter implements PinotCrypter {

    private int _initCalled = 0;
    private int _encryptCalled = 0;
    private int _decryptCalled = 0;
    private String _originalPath;
    private String _decryptedPath;

    @Override
    public void init(PinotConfiguration config) {
      _initCalled++;
    }

    @Override
    public void encrypt(File origFile, File encFile) {
      _encryptCalled++;
    }

    @Override
    public void decrypt(File origFile, File decFile) {
      _decryptCalled++;
      _originalPath = origFile.getPath();
      _decryptedPath = decFile.getPath();
    }
  }
}
