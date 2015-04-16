package com.linkedin.thirdeye.dashboard.views;

import io.dropwizard.views.View;

public class ExceptionView extends View {
  private final Throwable cause;

  public ExceptionView(Throwable cause) {
    super("exception.ftl");
    this.cause = cause;
  }

  public Throwable getCause() {
    return cause;
  }

  public StackTraceElement[] getStackTrace() {
    return cause.getStackTrace();
  }
}
