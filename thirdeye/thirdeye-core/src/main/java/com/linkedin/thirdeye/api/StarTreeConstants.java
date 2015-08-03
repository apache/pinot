package com.linkedin.thirdeye.api;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class StarTreeConstants
{
  public static final String STAR = "*";
  public static final String OTHER = "?";
  public static final String ALL = "!";

  public static final int STAR_VALUE = 0;
  public static final int OTHER_VALUE = 1;
  public static final int FIRST_VALUE = 2;

  public static final String CONFIG_FILE_NAME = "config.yml";
  public static final String TREE_FILE_NAME = "tree.bin";
  public static final String DATA_DIR_PREFIX = "data";
  public static final String KAFKA_CONFIG_FILE_NAME = "kafka.yml";
  public static final String METADATA_FILE_NAME = "metadata.properties";
  public static final String DIMENSION_STATS_FOLDER = "dimension_stats";

  public static final String INDEX_FILE_SUFFIX = ".idx";
  public static final String BUFFER_FILE_SUFFIX = ".buf";
  public static final String DICT_FILE_SUFFIX = ".dict";

  public static final String METRIC_STORE = "metricStore";
  public static final String DIMENSION_STORE = "dimensionStore";
  public static final String DICT_STORE = "dictStore";

  public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd-HHmmss");

  public enum Schedule {
    HOURLY {
      @Override
      public String getLowerSchedule() {
        return null;
      }

      @Override
      public DateTime getEndDateTime(DateTime start) {
        return start.plusHours(1);
      }
    },
    DAILY {
      @Override
      public String getLowerSchedule() {
        return HOURLY.name();
      }

      @Override
      public DateTime getEndDateTime(DateTime start) {
        return start.plusDays(1);
      }
    },
    MONTHLY {
      @Override
      public String getLowerSchedule() {
        return DAILY.name();
      }

      @Override
      public DateTime getEndDateTime(DateTime start) {
        return start.plusMonths(1);
      }
    };

    public abstract String getLowerSchedule();

    public abstract DateTime getEndDateTime(DateTime start);

  }
}
