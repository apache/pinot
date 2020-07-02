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
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SegmentFetcherFactoryTest {
  private static final String HTTP_PROTOCOL = "http";
  private static final String HTTPS_PROTOCOL = "https";
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
    Configuration config = new BaseConfiguration();
    config.addProperty("foo", "bar");
    config.addProperty("protocols", Arrays.asList(HTTP_PROTOCOL, HTTPS_PROTOCOL, TEST_PROTOCOL, "foo"));
    config.addProperty("http.foo", "bar");
    config.addProperty(TEST_PROTOCOL + SegmentFetcherFactory.SEGMENT_FETCHER_CLASS_KEY_SUFFIX,
        TestSegmentFetcher.class.getName());
    SegmentFetcherFactory.init(config);

    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTP_PROTOCOL).getClass(), HttpSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(HTTPS_PROTOCOL).getClass(), HttpsSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(FILE_PROTOCOL).getClass(), PinotFSSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher("foo").getClass(), PinotFSSegmentFetcher.class);
    assertEquals(SegmentFetcherFactory.getSegmentFetcher(TEST_PROTOCOL).getClass(), TestSegmentFetcher.class);

    TestSegmentFetcher testFileFetcher = (TestSegmentFetcher) SegmentFetcherFactory.getSegmentFetcher(TEST_PROTOCOL);
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 0);

    SegmentFetcherFactory.fetchSegmentToLocal(new URI(TEST_URI), new File("foo/bar"));
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 1);

    SegmentFetcherFactory.fetchSegmentToLocal(TEST_URI, new File("foo/bar"));
    assertEquals(testFileFetcher._initCalled, 1);
    assertEquals(testFileFetcher._fetchFileToLocalCalled, 2);
  }

  public static class TestSegmentFetcher implements SegmentFetcher {
    private int _initCalled = 0;
    private int _fetchFileToLocalCalled = 0;

    @Override
    public void init(Configuration config) {
      _initCalled++;
    }

    @Override
    public void fetchSegmentToLocal(URI uri, File dest)
        throws Exception {
      assertEquals(uri, new URI(TEST_URI));
      _fetchFileToLocalCalled++;
    }
  }
}
