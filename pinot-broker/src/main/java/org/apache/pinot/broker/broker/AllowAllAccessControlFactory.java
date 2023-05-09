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
package org.apache.pinot.broker.broker;

import java.util.Set;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.env.PinotConfiguration;


public class AllowAllAccessControlFactory extends AccessControlFactory {
  private final AccessControl _accessControl;

  public AllowAllAccessControlFactory() {
    _accessControl = new AllowAllAccessControl();
  }

  public void init(PinotConfiguration configuration) {
  }

  public AccessControl create() {
    return _accessControl;
  }

  private static class AllowAllAccessControl implements AccessControl {
    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      return true;
    }

    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, Set<String> tables) {
      return true;
    }
  }
}
