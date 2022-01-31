package org.apache.pinot.core.data.manager.realtime;

import com.google.common.cache.LoadingCache;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.annotations.Test;

import static org.apache.pinot.core.data.manager.realtime.RealtimeConsumptionRateManager.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class RealtimeConsumptionRateManagerTest {
  private static final int NUM_PARTITIONS_TOPIC_A = 10;
  private static final int NUM_PARTITIONS_TOPIC_B = 20;
  private static final Double RATE_LIMIT_FOR_ENTIRE_TOPIC = 50.0;
  private static final String TABLE_NAME = "table-XYZ";
  private static final double DELTA = 0.0001;
  private static final StreamConfig STREAM_CONFIG_A = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_B = mock(StreamConfig.class);
  private static final StreamConfig STREAM_CONFIG_C = mock(StreamConfig.class);
  private static RealtimeConsumptionRateManager consumptionRateManager;

  static {
    LoadingCache<StreamConfig, Integer> cache = mock(LoadingCache.class);
    try {
      when(cache.get(STREAM_CONFIG_A)).thenReturn(NUM_PARTITIONS_TOPIC_A);
      when(cache.get(STREAM_CONFIG_B)).thenReturn(NUM_PARTITIONS_TOPIC_B);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    when(STREAM_CONFIG_A.getTopicConsumptionRateLimit()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_B.getTopicConsumptionRateLimit()).thenReturn(Optional.of(RATE_LIMIT_FOR_ENTIRE_TOPIC));
    when(STREAM_CONFIG_C.getTopicConsumptionRateLimit()).thenReturn(Optional.empty());
    consumptionRateManager = new RealtimeConsumptionRateManager(cache);
  }

  @Test
  public void testCreateRateLimiter() {
    // topic A
    ConsumptionRateLimiter rateLimiter = consumptionRateManager.createRateLimiter(STREAM_CONFIG_A, TABLE_NAME);
    assertEquals(5.0, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic B
    rateLimiter = consumptionRateManager.createRateLimiter(STREAM_CONFIG_B, TABLE_NAME);
    assertEquals(2.5, ((RateLimiterImpl) rateLimiter).getRate(), DELTA);

    // topic C
    rateLimiter = consumptionRateManager.createRateLimiter(STREAM_CONFIG_C, TABLE_NAME);
    assertEquals(rateLimiter, NOOP_RATE_LIMITER);
  }

}