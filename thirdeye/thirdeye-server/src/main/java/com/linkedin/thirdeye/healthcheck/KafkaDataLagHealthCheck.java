package com.linkedin.thirdeye.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.linkedin.thirdeye.managed.ThirdEyeKafkaConsumerManager;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaStats;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaDataLagHealthCheck extends HealthCheck
{
  public static final String NAME = "kafkaDataLagCheck";

  private static final long MINIMUM_ACCEPTABLE_DATA_LAG_MILLIS = TimeUnit.MILLISECONDS.convert(3, TimeUnit.HOURS);

  private final ThirdEyeKafkaConsumerManager kafkaConsumerManager;

  public KafkaDataLagHealthCheck(ThirdEyeKafkaConsumerManager kafkaConsumerManager)
  {
    this.kafkaConsumerManager = kafkaConsumerManager;
  }

  @Override
  protected Result check() throws Exception
  {
    Map<String, Map<String, ThirdEyeKafkaStats>> stats = kafkaConsumerManager.getStats();

    StringBuilder errorMessage = new StringBuilder();

    for (Map.Entry<String, Map<String, ThirdEyeKafkaStats>> collectionEntry : stats.entrySet())
    {
      for (Map.Entry<String, ThirdEyeKafkaStats> streamEntry : collectionEntry.getValue().entrySet())
      {
        long currentTimeMillis = System.currentTimeMillis();
        long dataTimeMillis = streamEntry.getValue().getDataTimeMillis().get();

        if (currentTimeMillis - dataTimeMillis > MINIMUM_ACCEPTABLE_DATA_LAG_MILLIS)
        {
          errorMessage.append("collection=")
                      .append(collectionEntry.getKey())
                      .append(" kafkaTopic=")
                      .append(streamEntry.getKey())
                      .append(" currentTimeMillis=")
                      .append(currentTimeMillis)
                      .append(" dataTimeMillis=")
                      .append(dataTimeMillis)
                      .append(" difference of ")
                      .append(currentTimeMillis - dataTimeMillis)
                      .append(" is greater than acceptable data lag of ")
                      .append(MINIMUM_ACCEPTABLE_DATA_LAG_MILLIS)
                      .append("\n");
        }
      }
    }

    if (errorMessage.length() > 0)
    {
      return Result.unhealthy(errorMessage.toString());
    }

    return Result.healthy();
  }
}
