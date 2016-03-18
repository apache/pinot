package com.linkedin.thirdeye.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 * A third-eye client wrapper that limits the maximum number of connections.
 */
public class FlowControlledThirdEyeClient implements ThirdEyeClient {
  private static final Logger LOG = LoggerFactory.getLogger(FlowControlledThirdEyeClient.class);

  private final Semaphore requestPermits;
  private final ThirdEyeClient client;

  public FlowControlledThirdEyeClient(ThirdEyeClient client, int maxParallelRequests) {
    LOG.info("Creating FlowControlledThirdEyeClient from {} with {} max parallel requests", client,
        maxParallelRequests);
    this.client = client;
    this.requestPermits = new Semaphore(maxParallelRequests);
  }

  @Override
  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.execute(request);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getRawResponse(request);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public StarTreeConfig getStarTreeConfig(String collection) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getStarTreeConfig(collection);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public List<String> getCollections() throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getCollections();
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getSegmentDescriptors(collection);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public long getExpectedTimeBuckets(ThirdEyeRequest request) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getExpectedTimeBuckets(request);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public List<String> getExpectedTimestamps(ThirdEyeRequest request) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return client.getExpectedTimestamps(request);
    } finally {
      requestPermits.release();
    }
  }

  @Override
  public void clear() throws Exception {
    client.clear();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

}
