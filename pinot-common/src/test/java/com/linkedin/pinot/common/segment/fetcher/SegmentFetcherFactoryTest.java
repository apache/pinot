package com.linkedin.pinot.common.segment.fetcher;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationMap;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by jamesshao on 9/19/17.
 */
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
    conf.addProperty("pinot.server.segment.fetcher.test.class", "com.linkedin.pinot.common.segment.fetcher.testSegmentFetcher");
    SegmentFetcherFactory.getPreloadedSegments().put("hdfs", mockHdfsFetcher);
    SegmentFetcherFactory.getPreloadedSegments().put("http", mockHttpFetcher);
    SegmentFetcherFactory.getPreloadedSegments().put("https", mockHttpsFetcher);

    SegmentFetcherFactory.initSegmentFetcherFactory(conf);
    verify(mockHdfsFetcher, times(1))
        .init(eq(new ConfigurationMap(conf.subset("pinot.server.segment.fetcher.hdfs"))));
    verify(mockHttpFetcher, times(1))
        .init(eq(new ConfigurationMap(conf.subset("pinot.server.segment.fetcher.http"))));
    verify(mockHttpsFetcher, times(1))
        .init(eq(new HashMap<String, String>()));

    Assert.assertTrue(SegmentFetcherFactory.containsProtocol("test"));
    Assert.assertEquals(1, ((testSegmentFetcher)SegmentFetcherFactory.getPreloadedSegments().get("test")).init_called);
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