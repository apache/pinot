package com.linkedin.pinot.controller.api.reslet.resources;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.controller.ControllerConf;


public class PinotControllerHealthCheck extends ServerResource {
  private static final Logger logger = Logger.getLogger(PinotControllerHealthCheck.class);
  private final ControllerConf conf;
  private final String vip;

  public PinotControllerHealthCheck() throws IOException {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    vip = StringUtil.join("://", "http", StringUtil.join(":", conf.getControllerVipHost(), conf.getControllerPort()));
    logger.info("controller download url base is : " + vip);
  }

  @Override
  public Representation get() {
    Representation presentation = null;
    if (StringUtils.isNotBlank(vip)) {
      presentation = new StringRepresentation("GOOD");
    }
    return presentation;
  }

}
