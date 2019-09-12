package org.apache.pinot.benchmark.api.resources;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;


public class PinotBenchException extends WebApplicationException {
  public PinotBenchException(Logger logger, String message, Response.Status status) {
    this(logger, message, status.getStatusCode(), null);
  }

  public PinotBenchException(Logger logger, String message, int status) {
    this(logger, message, status, null);
  }

  public PinotBenchException(Logger logger, String message, Response.Status status, @Nullable Throwable e) {
    this(logger, message, status.getStatusCode(), e);
  }

  public PinotBenchException(Logger logger, String message, int status, @Nullable Throwable e) {
    super(message, status);
    if (status >= 300 && status < 500) {
      if (e == null) {
        logger.info(message);
      } else {
        logger.info(message + " exception: " + e.getMessage());
      }
    } else {
      if (e == null) {
        logger.error(message);
      } else {
        logger.error(message, e);
      }
    }
  }
}
