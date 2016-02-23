package com.linkedin.thirdeye.bootstrap;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.StarTreeConstants;

public class TestTimeConversion {
  @Test
  public void testTenMinutesSinceEpoch() {
    long sourceDuration = 2360321;
    TimeUnit sourceUnit = TimeUnit.MINUTES;
    long convert = TimeUnit.MINUTES.convert(sourceDuration, sourceUnit);
    System.out.println(convert);
  }

  @Test
  public void testTimeFormatter() {

    StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime("2015-04-26-180000");
  }
}
