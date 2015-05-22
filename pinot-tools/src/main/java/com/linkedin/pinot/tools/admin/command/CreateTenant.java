/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.admin.command;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.Tenant;
import com.linkedin.pinot.common.utils.TenantRole;
import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;


public class CreateTenant extends AbstractBaseCommand implements Command {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTenant.class);

  @Option(name = "-url", required = true, metaVar = "<http>", usage = "Http address of Controller (with port).")
  private String url = null;

  @Option(name = "-name", required = true, metaVar = "<string>", usage = "Name of the tenant to be created")
  private String name;

  @Option(name = "-role", required = false, metaVar = "<AVRO/CSV/JSON>", usage = "Input data format.")
  private TenantRole role;

  @Option(name = "-instances", required = true, metaVar = "<string>", usage = "Name of the resource.")
  private int instances;

  @Option(name = "-offline", required = false, metaVar = "<string>", usage = "Name of the segment.")
  private int offline;

  @Option(name = "-realtime", required = false, metaVar = "<string>", usage = "File containing schema for data.")
  private int realtime;

  @Option(name = "-help", required = false, help = true, usage = "Print this message.")
  private boolean help = false;

  @Override
  public boolean execute() throws Exception {
    Tenant t =
        new Tenant.TenantBuilder(name).setType(role).setTotalInstances(instances).setOfflineInstances(offline)
            .setRealtimeInstances(realtime).build();

    String res =
        AbstractBaseCommand.sendPostRequest(ControllerRequestURLBuilder.baseUrl(url).forTenantCreate(), t.toString());

    LOGGER.info(res);
    System.out.print(res);
    return true;
  }

  @Override
  public boolean getHelp() {
    return help;
  }

  public static void main(String[] args) {

  }
}
