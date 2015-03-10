package com.linkedin.thirdeye.bootstrap;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class TestTimeConversion {
  @Test
  public void testTenMinutesSinceEpoch() {
     long sourceDuration = 2360321;
    TimeUnit sourceUnit = TimeUnit.MINUTES;
    long convert = TimeUnit.MINUTES.convert(sourceDuration, sourceUnit);
    System.out.println(convert);

  }
}
