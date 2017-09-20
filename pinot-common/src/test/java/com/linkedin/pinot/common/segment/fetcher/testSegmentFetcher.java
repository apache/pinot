package com.linkedin.pinot.common.segment.fetcher;

import java.io.File;
import java.util.Map;

/**
 * Created by jamesshao on 9/20/17.
 */
public class testSegmentFetcher implements SegmentFetcher {
  public int init_called = 0;

  @Override
  public void init(Map<String, String> configs) {
    init_called++;
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {

  }
}
