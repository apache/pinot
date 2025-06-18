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
package org.apache.pinot.controller.api.access;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Basic Authentication based on http headers. Configured via the "controller.admin.access.control" family of
 * properties.
 *
 * <pre>
 *     Example:
 *     controller.admin.access.control.principals=admin123,user456
 *     controller.admin.access.control.principals.admin123.password=verysecret
 *     controller.admin.access.control.principals.user456.password=kindasecret
 *     controller.admin.access.control.principals.user456.tables=stuff,lessImportantStuff
 *     controller.admin.access.control.principals.user456.permissions=read,update
 * </pre>
 */
public class BasicAuthAccessControlFactory implements AccessControlFactory {
  private static final String PREFIX = "controller.admin.access.control.principals";

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration) {
    _accessControl = new BasicAuthAccessControl(BasicAuthUtils.extractBasicAuthPrincipals(configuration, PREFIX));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl extends AbstractBasicAuthAccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    protected Optional<BasicAuthPrincipal> getPrincipal(HttpHeaders headers) {
      return BasicAuthUtils.getPrincipal(getTokens(headers), _token2principal);
    }
  }
}
