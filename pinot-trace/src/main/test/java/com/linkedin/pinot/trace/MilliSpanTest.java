package com.linkedin.pinot.trace;

import org.testng.annotations.Test;


public class MilliSpanTest {

  @Test
  public void testMilliSpanBuilder() {
    MilliSpan.newBuilder(123).startTime(12).build();
  }
}