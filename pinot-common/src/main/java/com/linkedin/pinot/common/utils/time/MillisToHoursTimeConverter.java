package com.linkedin.pinot.common.utils.time;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import com.linkedin.pinot.common.data.GranularitySpec;


public class MillisToHoursTimeConverter implements TimeConverter {

  GranularitySpec incoming;
  GranularitySpec outgoing;

  public MillisToHoursTimeConverter(GranularitySpec incoming, GranularitySpec outgoing) {
    incoming = this.incoming;
    outgoing = this.outgoing;
  }

  @Override
  public long convert(Object incomingTime) {
    long incomingInLong = -1;
    switch (incoming.getdType()) {
      case INT:
        incomingInLong = new Long((Integer) incomingTime).longValue();
        break;
      case LONG:
        incomingInLong = ((Long) incomingTime).longValue();
    }
    return TimeUnit.MILLISECONDS.toHours(incomingInLong);
  }

  @Override
  public DateTime getDataTimeFrom(long incoming) {
    return new DateTime(TimeUnit.HOURS.toMillis(incoming));
  }
}
