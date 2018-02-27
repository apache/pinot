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

package com.linkedin.pinot.broker.broker;

import com.linkedin.pinot.broker.api.AccessControl;
import com.linkedin.pinot.broker.api.RequesterIdentity;
import com.linkedin.pinot.common.request.BrokerRequest;
import org.apache.commons.configuration.Configuration;


public class AllowAllAccessControlFactory extends AccessControlFactory {
  private final AccessControl _accessControl;
  public AllowAllAccessControlFactory() {
    _accessControl = new AllowAllAccessControl();
  }

  public void init(Configuration configuration) {
  }

  public AccessControl create() {
    return _accessControl;
  }

  private static class AllowAllAccessControl implements AccessControl {
    @Override
    public boolean hasAccess(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      return true;
    }
  }
}
