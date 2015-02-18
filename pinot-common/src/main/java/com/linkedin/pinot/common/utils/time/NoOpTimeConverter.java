package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.linkedin.pinot.common.data.TimeGranularitySpec;


public class NoOpTimeConverter implements TimeConverter {
  TimeGranularitySpec incomingTimeGranularitySpec;
  TimeGranularitySpec outgoingTimeGranularitySpec;

  public NoOpTimeConverter(TimeGranularitySpec incoming) {
    this.incomingTimeGranularitySpec = incoming;
    this.outgoingTimeGranularitySpec = incoming;
  }

  @Override
  public long convert(Object incoming) {
    long incomingInLong = -1;
    switch (incomingTimeGranularitySpec.getdType()) {
      case INT:
        incomingInLong = new Long((Integer) incoming).longValue();
        break;
      case LONG:
        incomingInLong = ((Long) incoming).longValue();
    }

    return incomingInLong;
  }

  public DateTime getDataTimeFrom(long incoming) {
    switch (incomingTimeGranularitySpec.getTimeType()) {
      case HOURS:
        long millisFromHours = TimeUnit.HOURS.toMillis(incoming);
        return new DateTime(millisFromHours);
      case DAYS:
        long millisFromDays = TimeUnit.DAYS.toMillis(incoming);
        return new DateTime(millisFromDays);
      case MILLISECONDS:
        return new DateTime(incoming);
      case MINUTES:
        long millisFromMinutes = TimeUnit.MINUTES.toMillis(incoming);
        return new DateTime(millisFromMinutes);
      default:
        return null;
    }
  }
}
