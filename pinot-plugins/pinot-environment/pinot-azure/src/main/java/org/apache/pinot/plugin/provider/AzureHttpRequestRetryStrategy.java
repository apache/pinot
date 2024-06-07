package org.apache.pinot.plugin.provider;

import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.core5.util.TimeValue;


public class AzureHttpRequestRetryStrategy extends DefaultHttpRequestRetryStrategy {
  public AzureHttpRequestRetryStrategy(
      final int maxRetries) {
    super(maxRetries, TimeValue.ofSeconds(1));
  }
}
