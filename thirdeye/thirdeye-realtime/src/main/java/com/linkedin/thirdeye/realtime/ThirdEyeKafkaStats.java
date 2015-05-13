package com.linkedin.thirdeye.realtime;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

public class ThirdEyeKafkaStats {
  public static final String RECORDS_ADDED = "recordsAdded";
  public static final String RECORDS_ERROR = "recordsError";
  public static final String RECORDS_SKIPPED_INVALID = "recordsSkippedInvalid";
  public static final String RECORDS_SKIPPED_EXPIRED = "recordsSkippedExpired";
  public static final String BYTES_READ = "bytesRead";
  public static final String LAST_PERSIST_TIME_MILLIS = "lastPersistTimeMillis";
  public static final String TIME_SINCE_LAST_PERSIST_MILLIS = "timeSinceLastPersistMillis";
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

  public ThirdEyeKafkaStats(String collection, String topic, MetricRegistry metricRegistry) {
    String recordsAddedName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        RECORDS_ADDED);
    metricRegistry.remove(recordsAddedName);
    this.recordsAdded = metricRegistry.meter(recordsAddedName);

    String recordsErrorName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        RECORDS_ERROR);
    metricRegistry.remove(recordsErrorName);
    this.recordsError = metricRegistry.meter(recordsErrorName);

    String recordsSkippedInvalidName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        RECORDS_SKIPPED_INVALID);
    metricRegistry.remove(recordsSkippedInvalidName);
    this.recordsSkippedInvalid = metricRegistry.meter(recordsSkippedInvalidName);

    String recordsSkippedExpiredName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        RECORDS_SKIPPED_EXPIRED);
    metricRegistry.remove(recordsSkippedExpiredName);
    this.recordsSkippedExpired = metricRegistry.meter(recordsSkippedExpiredName);

    String bytesReadName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        BYTES_READ);
    metricRegistry.remove(bytesReadName);
    this.bytesRead = metricRegistry.meter(bytesReadName);

    final String lastPersistTimeMillisName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        LAST_PERSIST_TIME_MILLIS);
    metricRegistry.remove(lastPersistTimeMillisName);
    metricRegistry.register(lastPersistTimeMillisName, new Gauge<Long>() {
      @Override
      public Long getValue() {
        return lastPersistTimeMillis.get();
      }
    });

    String lastConsumedRecordTimeMillisName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        LAST_CONSUMED_RECORD_TIME_MILLIS);
    metricRegistry.remove(lastConsumedRecordTimeMillisName);
    metricRegistry.register(lastConsumedRecordTimeMillisName, new Gauge<Long>() {
      @Override
      public Long getValue() {
        return lastConsumedRecordTimeMillis.get();
      }
    });

    String dataTimeMillisName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        DATA_TIME_MILLIS);
    metricRegistry.remove(dataTimeMillisName);
    metricRegistry.register(dataTimeMillisName, new Gauge<Long>() {
      @Override
      public Long getValue() {
        return dataTimeMillis.get();
      }
    });

    String dataLagMillisName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        DATA_LAG_MILLIS);
    metricRegistry.remove(dataLagMillisName);
    metricRegistry.register(dataLagMillisName, new Gauge<Long>() {
      @Override
      public Long getValue() {
        return System.currentTimeMillis() - dataTimeMillis.get();
      }
    });

    final String timeSinceLastPersistMillisName = MetricRegistry.name(ThirdEyeKafkaStats.class,
        collection,
        topic,
        TIME_SINCE_LAST_PERSIST_MILLIS);
    metricRegistry.remove(timeSinceLastPersistMillisName);
    metricRegistry.register(timeSinceLastPersistMillisName, new Gauge<Long>() {
      @Override
      public Long getValue() {
        return System.currentTimeMillis() - lastPersistTimeMillis.get();
      }
    });
  }

  public AtomicLong getLastPersistTimeMillis() {
    return lastPersistTimeMillis;
  }

  public AtomicLong getLastConsumedRecordTimeMillis() {
    return lastConsumedRecordTimeMillis;
  }

  public AtomicLong getDataTimeMillis() {
    return dataTimeMillis;
  }

  public Meter getRecordsAdded() {
    return recordsAdded;
  }

  public Meter getRecordsError() {
    return recordsError;
  }

  public Meter getRecordsSkippedInvalid() {
    return recordsSkippedInvalid;
  }

  public Meter getRecordsSkippedExpired() {
    return recordsSkippedExpired;
  }

  public Meter getBytesRead() {
    return bytesRead;
  }
}
