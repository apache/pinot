/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.CommonConstants;
import java.io.File;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class SegmentFetcherFactoryTest {

  @BeforeTest
  public void setUp() {
    SegmentFetcherFactory.getPreloadSegmentFetchers().clear();
  }

  @Test
  public void testInitSegmentFetcherFactory() throws Exception {
    SegmentFetcher mockHdfsFetcher = mock(SegmentFetcher.class);
    SegmentFetcher mockHttpFetcher = mock(SegmentFetcher.class);
    SegmentFetcher mockHttpsFetcher = mock(SegmentFetcher.class);
    SegmentFetcher replacableFetcher = mock(SegmentFetcher.class);
    final String replacableProto = "replacable";

    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("something", "abc");
    conf.addProperty("pinot.server.segment.fetcher.hdfs.hadoop.conf.path", "file:///somewhere/folder");
    conf.addProperty("pinot.server.segment.fetcher.http.other", "otherconfig");
    conf.addProperty("pinot.server.segment.fetcher.http2.more_other", "some-other");
    conf.addProperty("pinot.server.segment.fetcher.test.class", TestSegmentFetcher.class.getName());
    conf.addProperty("pinot.server.segment.fetcher." + replacableProto + ".class", ReplacedFetcher.class.getName());
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("hdfs", mockHdfsFetcher);
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("http", mockHttpFetcher);
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("https", mockHttpsFetcher);
    SegmentFetcherFactory.getPreloadSegmentFetchers().put(replacableProto, replacableFetcher);

    SegmentFetcherFactory.initSegmentFetcherFactory(
        conf.subset(CommonConstants.Server.PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY));
    ArgumentCaptor<Configuration> captor = ArgumentCaptor.forClass(Configuration.class);
    verify(mockHdfsFetcher).init(captor.capture());
    Assert.assertEquals(captor.getValue().getString("hadoop.conf.path"), "file:///somewhere/folder");
    Assert.assertEquals(captor.getValue().getString("other"), null);
    Assert.assertTrue(SegmentFetcherFactory.containsProtocol("test"));
    Assert.assertEquals(
        ((TestSegmentFetcher) SegmentFetcherFactory.getPreloadSegmentFetchers().get("test")).init_called, 1);
    ReplacedFetcher fetcher = (ReplacedFetcher) SegmentFetcherFactory.getPreloadSegmentFetchers().get(replacableProto);
    Assert.assertEquals(fetcher.getInitCalled(), 1);
  }

  @Test
  public void testGetSegmentFetcherBasedOnURI() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    configuration.addProperty("https.ssl.server.enable-verification", "false");
    SegmentFetcherFactory.initSegmentFetcherFactory(configuration);
    Assert.assertTrue(
        SegmentFetcherFactory.getSegmentFetcherBasedOnURI("hdfs:///something/wer") instanceof HdfsSegmentFetcher);
    Assert.assertTrue(
        SegmentFetcherFactory.getSegmentFetcherBasedOnURI("http://something:wer:") instanceof HttpSegmentFetcher);
    Assert.assertTrue(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("https://") instanceof HttpsSegmentFetcher);
    Assert.assertTrue(
        SegmentFetcherFactory.getSegmentFetcherBasedOnURI("file://a/asdf/wer/fd/e") instanceof LocalFileSegmentFetcher);
  }

  @Test
  public void testGetSegmentFetcherBasedOnURIFailed() throws Exception {
    Configuration configuration = new PropertiesConfiguration();
    SegmentFetcherFactory.initSegmentFetcherFactory(configuration);
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI("");
      Assert.fail();
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI("abc:///something");
      Assert.fail();
    } catch (IllegalStateException e) {
      // Expected
    }
    try {
      // If HttpsSegmentFetcher is not configured correctly, it fails to initialize.
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI("https://");
      Assert.fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  public static class ReplacedFetcher implements SegmentFetcher {

    public int initCalled = 0;

    @Override
    public void init(Configuration configs) {
      initCalled++;
    }

    public int getInitCalled() {
      return initCalled;
    }

    @Override
    public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {
    }

    @Override
    public Set<String> getProtectedConfigKeys() {
      return Collections.<String>emptySet();
    }
  }
}