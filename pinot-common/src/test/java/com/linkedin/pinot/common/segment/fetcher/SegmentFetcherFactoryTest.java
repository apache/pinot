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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SegmentFetcherFactoryTest {

  @Test
  public void testInitSegmentFetcherFactory() throws Exception {
    SegmentFetcher mockHdfsFetcher = mock(SegmentFetcher.class);
    SegmentFetcher mockHttpFetcher = mock(SegmentFetcher.class);
    SegmentFetcher mockHttpsFetcher = mock(SegmentFetcher.class);

    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("something", "abc");
    conf.addProperty("pinot.server.segment.fetcher.hdfs.hadoop.conf.path", "file:///somewhere/folder");
    conf.addProperty("pinot.server.segment.fetcher.http.other", "otherconfig");
    conf.addProperty("pinot.server.segment.fetcher.http2.more_other", "some-other");
    conf.addProperty("pinot.server.segment.fetcher.test.class", "com.linkedin.pinot.common.segment.fetcher.TestSegmentFetcher");
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("hdfs", mockHdfsFetcher);
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("http", mockHttpFetcher);
    SegmentFetcherFactory.getPreloadSegmentFetchers().put("https", mockHttpsFetcher);

    SegmentFetcherFactory.initSegmentFetcherFactory(conf);
    ArgumentCaptor<Configuration> captor = ArgumentCaptor.forClass(Configuration.class);
    verify(mockHdfsFetcher).init(captor.capture());
    Assert.assertEquals(captor.getValue().getString("hadoop.conf.path"), "file:///somewhere/folder");
    Assert.assertEquals(captor.getValue().getString("other"), null);
    Assert.assertTrue(SegmentFetcherFactory.containsProtocol("test"));
    Assert.assertEquals(1, ((TestSegmentFetcher)SegmentFetcherFactory.getPreloadSegmentFetchers().get("test")).init_called);
  }

  @Test
  public void testGetSegmentFetcherBasedOnURI() throws Exception {
    Assert.assertTrue(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("hdfs:///something/wer") instanceof HdfsSegmentFetcher);
    Assert.assertTrue(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("http://something:wer:") instanceof HttpSegmentFetcher);
    Assert.assertTrue(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("https://") instanceof HttpSegmentFetcher);
    Assert.assertTrue(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("file://a/asdf/wer/fd/e") instanceof LocalFileSegmentFetcher);

    Assert.assertNull(SegmentFetcherFactory.getSegmentFetcherBasedOnURI("abc:///something"));
  }

  @Test
  public void testGetSegmentFetcherBasedOnURIFailed() throws Exception {
    try {
      SegmentFetcherFactory.getSegmentFetcherBasedOnURI("");
    } catch (UnsupportedOperationException ex) {
      return;
    }
    Assert.fail();
  }
}