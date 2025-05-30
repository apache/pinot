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
package org.apache.pinot.server.access;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.spi.auth.server.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;


public class BasicAuthAccessFactory implements AccessControlFactory {
  private static final String PREFIX = "principals";

  private AccessControl _accessControl;

  @Override
  public void init(PinotConfiguration configuration) {
    _accessControl = new BasicAuthAccessControl(BasicAuthUtils.extractBasicAuthPrincipals(configuration, PREFIX));
  }

  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using metadata-based basic grpc authentication
   */
  private static class BasicAuthAccessControl extends AbstractBasicAuthAccessControl {
    private final Map<String, BasicAuthPrincipal> _token2principal;

    public BasicAuthAccessControl(Collection<BasicAuthPrincipal> principals) {
      _token2principal = principals.stream().collect(Collectors.toMap(BasicAuthPrincipal::getToken, p -> p));
    }

    @Override
    protected Optional<? extends BasicAuthPrincipal> getPrincipal(RequesterIdentity requesterIdentity) {
      return BasicAuthUtils.getPrincipal(getTokens(requesterIdentity), _token2principal);
    }
  }
}
