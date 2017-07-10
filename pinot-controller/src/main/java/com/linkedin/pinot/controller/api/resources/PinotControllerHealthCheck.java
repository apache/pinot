/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.controller.ControllerConf;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ServerResource;


public class PinotControllerHealthCheck extends ServerResource {
  private final ControllerConf conf;
  private final String vip;

  public PinotControllerHealthCheck() throws IOException {
    conf = (ControllerConf) getApplication().getContext().getAttributes().get(ControllerConf.class.toString());
    vip = conf.generateVipUrl();
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
