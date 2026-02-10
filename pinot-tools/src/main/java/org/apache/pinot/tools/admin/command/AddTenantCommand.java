/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.admin.command;

import java.net.URI;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "AddTenant", mixinStandardHelpOptions = true)
public class AddTenantCommand extends AbstractDatabaseBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTenantCommand.class);

  @CommandLine.Option(names = {"-name"}, required = true, description = "Name of the tenant to be created")
  private String _name;

  @CommandLine.Option(names = {"-role"}, required = true, description = "Tenant role (broker/server).")
  private TenantRole _role;

  @CommandLine.Option(names = {"-instanceCount"}, required = true, description = "Number of instances.")
  private int _instanceCount;

  @CommandLine.Option(names = {"-offlineInstanceCount"}, required = false,
      description = "Number of offline instances.")
  private Integer _offlineInstanceCount;

  @CommandLine.Option(names = {"-realTimeInstanceCount"}, required = false,
      description = "Number of realtime instances.")
  private Integer _realtimeInstanceCount;

  public AddTenantCommand setName(String name) {
    _name = name;
    return this;
  }

  public AddTenantCommand setRole(TenantRole role) {
    _role = role;
    return this;
  }

  public AddTenantCommand setInstances(int instances) {
    _instanceCount = instances;
    return this;
  }

  public AddTenantCommand setOffline(int offline) {
    _offlineInstanceCount = offline;
    return this;
  }

  public AddTenantCommand setRealtime(int realtime) {
    _realtimeInstanceCount = realtime;
    return this;
  }

  public AddTenantCommand setControllerUrl(String url) {
    URI uri = URI.create(url);
    if (uri.getScheme() != null) {
      _controllerProtocol = uri.getScheme();
    }
    if (uri.getHost() != null) {
      _controllerHost = uri.getHost();
    }
    if (uri.getPort() > 0) {
      _controllerPort = Integer.toString(uri.getPort());
    }
    return this;
  }

  public AddTenantCommand setExecute(boolean exec) {
    super.setExecute(exec);
    return this;
  }

  public AddTenantCommand setAuthProvider(AuthProvider authProvider) {
    super.setAuthProvider(authProvider);
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    if (!_exec) {
      LOGGER.warn("Dry Running Command: {}", toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    LOGGER.info("Executing command: {}", toString());

    if (_role == TenantRole.SERVER) {
      if (_offlineInstanceCount == null || _realtimeInstanceCount == null) {
        throw new IllegalArgumentException("-offlineInstanceCount and -realTimeInstanceCount should be set when "
            + "creating a server tenant.");
      }
    }

    Tenant tenant = new Tenant(_role, _name, _instanceCount, _offlineInstanceCount, _realtimeInstanceCount);
    try (PinotAdminClient adminClient = getPinotAdminClient()) {
      String res = adminClient.getTenantClient().createTenant(tenant.toJsonString());
      LOGGER.info(res);
      System.out.print(res);
      return true;
    } catch (PinotAdminException e) {
      LOGGER.error("Failed to create tenant {}", _name, e);
      return false;
    }
  }

  @Override
  public String getName() {
    return "AddTenant";
  }

  @Override
  public String toString() {
    String retString = ("AddTenant -name " + _name + " -role " + _role + " -instanceCount "
        + _instanceCount + " -offlineInstanceCount " + _offlineInstanceCount + " -realTimeInstanceCount "
        + _realtimeInstanceCount);
    return retString + super.toString();
  }

  @Override
  public String description() {
    return "Add tenant as per the specification provided.";
  }
}
