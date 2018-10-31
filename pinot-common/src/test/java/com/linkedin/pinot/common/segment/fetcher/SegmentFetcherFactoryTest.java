/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.segment.fetcher;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentFetcherFactoryTest {
  private SegmentFetcherFactory _segmentFetcherFactory;

  @BeforeMethod
  public void setUp() throws Exception {
    // Use reflection to get a new segment fetcher factory
    Constructor<SegmentFetcherFactory> constructor = SegmentFetcherFactory.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    _segmentFetcherFactory = constructor.newInstance();
  }

  @Test
  public void testDefaultSegmentFetcherFactory() throws Exception {
    _segmentFetcherFactory.init(new PropertiesConfiguration());
    Assert.assertTrue(_segmentFetcherFactory.containsProtocol("file"));
    Assert.assertTrue(_segmentFetcherFactory.containsProtocol("http"));
    Assert.assertFalse(_segmentFetcherFactory.containsProtocol("https"));
    Assert.assertFalse(_segmentFetcherFactory.containsProtocol("hdfs"));
  }

  @Test
  public void testCustomizedSegmentFetcherFactory() throws Exception {
    Configuration config = new PropertiesConfiguration();
    config.addProperty("something", "abc");
    config.addProperty("protocols", Arrays.asList("http", "https", "test"));
    config.addProperty("http.other", "some config");
    config.addProperty("https.class", NoOpFetcher.class.getName());
    config.addProperty("test.class", TestSegmentFetcher.class.getName());
    _segmentFetcherFactory.init(config);

    Assert.assertTrue(_segmentFetcherFactory.containsProtocol("http"));
    Assert.assertTrue(_segmentFetcherFactory.containsProtocol("https"));
    Assert.assertTrue(_segmentFetcherFactory.containsProtocol("test"));
    SegmentFetcher testSegmentFetcher = _segmentFetcherFactory.getSegmentFetcherBasedOnURI("test://something");
    Assert.assertTrue(testSegmentFetcher instanceof TestSegmentFetcher);
    Assert.assertEquals(((TestSegmentFetcher) testSegmentFetcher).getInitCalled(), 1);
  }

  @Test
  public void testGetSegmentFetcherBasedOnURI() throws Exception {
    _segmentFetcherFactory.init(new PropertiesConfiguration());

    Assert.assertTrue(
        _segmentFetcherFactory.getSegmentFetcherBasedOnURI("http://something:wer:") instanceof HttpSegmentFetcher);
    Assert.assertTrue(_segmentFetcherFactory.getSegmentFetcherBasedOnURI(
        "file://a/asdf/wer/fd/e") instanceof PinotFSSegmentFetcher);
    Assert.assertNull(_segmentFetcherFactory.getSegmentFetcherBasedOnURI("abc:///something"));
    Assert.assertNull(_segmentFetcherFactory.getSegmentFetcherBasedOnURI("https://something"));
    try {
      _segmentFetcherFactory.getSegmentFetcherBasedOnURI("https:");
      Assert.fail();
    } catch (URISyntaxException e) {
      // Expected
    }
  }

  public static class TestSegmentFetcher implements SegmentFetcher {
    public int initCalled = 0;

    @Override
    public void init(Configuration configs) {
      initCalled++;
    }

    public int getInitCalled() {
      return initCalled;
    }

    @Override
    public void fetchSegmentToLocal(String uri, File tempFile) {
    }

    @Override
    public Set<String> getProtectedConfigKeys() {
      return Collections.emptySet();
    }
  }
}