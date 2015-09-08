package com.linkedin.thirdeye.client;

import java.util.Map;
import java.util.concurrent.Semaphore;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;

/**
 * A default third-eye client that limits the maximum number of connections.
 */
public class FlowControlledDefaultThirdEyeClient extends DefaultThirdEyeClient {

  private final Semaphore requestPermits;

  public FlowControlledDefaultThirdEyeClient(String hostname, int port, DefaultThirdEyeClientConfig config,
      int maxParallelRequests) {
    super(hostname, port, config);
    requestPermits = new Semaphore(maxParallelRequests);
  }

  public FlowControlledDefaultThirdEyeClient(String hostname, int port, int maxParallelRequests) {
    super(hostname, port);
    requestPermits = new Semaphore(maxParallelRequests);
  }

  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    requestPermits.acquireUninterruptibly();
    try {
      return super.execute(request);
    } finally {
      requestPermits.release();
    }
  }

}
