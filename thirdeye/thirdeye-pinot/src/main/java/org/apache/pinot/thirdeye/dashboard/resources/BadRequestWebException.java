package org.apache.pinot.thirdeye.dashboard.resources;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class BadRequestWebException extends WebApplicationException {

  public BadRequestWebException() {
    this("Bad or Malformed Request");
  }

  public BadRequestWebException(Exception e) {
    super(Response
        .status(Status.BAD_REQUEST)
        .entity(e.getMessage())
        .type(MediaType.TEXT_PLAIN)
        .build());
  }

  public BadRequestWebException(String message) {
    super(Response
        .status(Status.BAD_REQUEST)
        .entity(message)
        .type(MediaType.TEXT_PLAIN)
        .build());
  }
}
