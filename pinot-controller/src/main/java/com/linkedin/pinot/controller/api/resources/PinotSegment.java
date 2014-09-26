package com.linkedin.pinot.controller.api.resources;

import org.apache.log4j.Logger;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;



/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 24, 2014
 */
public class PinotSegment extends ServerResource {
  private static final Logger logger = Logger.getLogger(PinotSegment.class);

  public String getresource() {
    return "Got it!";
  }

  @Override
  public Representation get() {
    StringRepresentation presentation = null;
    try {
      // final String clusterName = (String) getRequest().getAttributes().get("clusterName");
      presentation = new StringRepresentation("this is a string");
    } catch (final Exception e) {
      logger.error(e);
    }
    return presentation;
  }
}
