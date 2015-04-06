package com.linkedin.thirdeye.realtime;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

public class ThirdEyeKafkaStats
{
  public static final String RECORDS_ADDED = "recordsAdded";
  public static final String RECORDS_ERROR = "recordsError";
  public static final String RECORDS_SKIPPED_INVALID = "recordsSkippedInvalid";
  public static final String RECORDS_SKIPPED_EXPIRED = "recordsSkippedExpired";
  public static final String BYTES_READ = "bytesRead";
  public static final String LAST_PERSIST_TIME_MILLIS = "lastPersistTimeMillis";
  public static final String LAST_CONSUMED_RECORD_TIME_MILLIS = "lastConsumedRecordTimeMillis";
  public static final String DATA_TIME_MILLIS = "dataTimeMillis";
  public static final String DATA_LAG_MILLIS = "dataLagMillis";

  private final AtomicLong lastPersistTimeMillis = new AtomicLong(System.currentTimeMillis());
  private final AtomicLong lastConsumedRecordTimeMillis = new AtomicLong(-1);
  private final AtomicLong dataTimeMillis = new AtomicLong(-1);
  private final Meter recordsAdded;
  private final Meter recordsError;
  private final Meter recordsSkippedInvalid;
  private final Meter recordsSkippedExpired;
  private final Meter bytesRead;

  public ThirdEyeKafkaStats(String collection, String topic, MetricRegistry metricRegistry)
  {
    this.recordsAdded = metricRegistry.meter(
            MetricRegistry.name(ThirdEyeKafkaStats.class,
                                collection,
                                topic,
                                RECORDS_ADDED));

    this.recordsError = metricRegistry.meter(
            MetricRegistry.name(ThirdEyeKafkaStats.class,
                                collection,
                                topic,
                                RECORDS_ERROR));

    this.recordsSkippedInvalid = metricRegistry.meter(
            MetricRegistry.name(ThirdEyeKafkaStats.class,
                                collection,
                                topic,
                                RECORDS_SKIPPED_INVALID));

    this.recordsSkippedExpired = metricRegistry.meter(
            MetricRegistry.name(ThirdEyeKafkaStats.class,
                                collection,
                                topic,
                                RECORDS_SKIPPED_EXPIRED));

    this.bytesRead = metricRegistry.meter(
            MetricRegistry.name(ThirdEyeKafkaStats.class,
                                collection,
                                topic,
                                BYTES_READ));

    metricRegistry.register(MetricRegistry.name(ThirdEyeKafkaStats.class,
                                                collection,
                                                topic,
                                                LAST_PERSIST_TIME_MILLIS), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        return lastPersistTimeMillis.get();
      }
    });

    metricRegistry.register(MetricRegistry.name(ThirdEyeKafkaStats.class,
                                                collection,
                                                topic,
                                                LAST_CONSUMED_RECORD_TIME_MILLIS), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        return lastConsumedRecordTimeMillis.get();
      }
    });

    metricRegistry.register(MetricRegistry.name(ThirdEyeKafkaStats.class,
                                                collection,
                                                topic,
                                                DATA_TIME_MILLIS), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        return dataTimeMillis.get();
      }
    });

    metricRegistry.register(MetricRegistry.name(ThirdEyeKafkaStats.class,
                                                collection,
                                                topic,
                                                DATA_LAG_MILLIS), new Gauge<Long>()
    {
      @Override
      public Long getValue()
      {
        return System.currentTimeMillis() - dataTimeMillis.get();
      }
    });
  }

  public AtomicLong getLastPersistTimeMillis()
  {
    return lastPersistTimeMillis;
  }

  public AtomicLong getLastConsumedRecordTimeMillis()
  {
    return lastConsumedRecordTimeMillis;
  }

  public AtomicLong getDataTimeMillis()
  {
    return dataTimeMillis;
  }

  public Meter getRecordsAdded()
  {
    return recordsAdded;
  }

  public Meter getRecordsError()
  {
    return recordsError;
  }

  public Meter getRecordsSkippedInvalid()
  {
    return recordsSkippedInvalid;
  }

  public Meter getRecordsSkippedExpired()
  {
    return recordsSkippedExpired;
  }

  public Meter getBytesRead()
  {
    return bytesRead;
  }
}
