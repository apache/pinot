package com.linkedin.thirdeye.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.linkedin.thirdeye.managed.KafkaConsumerManager;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaStats;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerLagHealthCheck extends HealthCheck {
  public static final String NAME = "kafkaConsumerLagCheck";

  private static final long MINIMUM_ACCEPTABLE_CONSUMER_LAG_MILLIS =
      TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private final KafkaConsumerManager kafkaConsumerManager;

  public KafkaConsumerLagHealthCheck(KafkaConsumerManager kafkaConsumerManager) {
    this.kafkaConsumerManager = kafkaConsumerManager;
  }

  @Override
  protected Result check() throws Exception {
    Map<String, ThirdEyeKafkaStats> stats = kafkaConsumerManager.getStats();

    StringBuilder errorMessage = new StringBuilder();

    for (Map.Entry<String, ThirdEyeKafkaStats> entry : stats.entrySet()) {
      long currentTimeMillis = System.currentTimeMillis();
      long consumerTimeMillis = entry.getValue().getLastConsumedRecordTimeMillis().get();

      if (currentTimeMillis - consumerTimeMillis > MINIMUM_ACCEPTABLE_CONSUMER_LAG_MILLIS) {
        errorMessage.append("collection=").append(entry.getKey()).append(" currentTimeMillis=")
            .append(currentTimeMillis).append(" consumerTimeMillis=").append(consumerTimeMillis)
            .append(" difference of ").append(currentTimeMillis - consumerTimeMillis)
            .append(" is greater than acceptable data lag of ")
            .append(MINIMUM_ACCEPTABLE_CONSUMER_LAG_MILLIS).append("\n");
      }
    }

    if (errorMessage.length() > 0) {
      return Result.unhealthy(errorMessage.toString());
    }

    return Result.healthy();
  }
}
