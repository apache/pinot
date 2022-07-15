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

package org.apache.pinot.common.helix;

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Pinot extended Instance Config for pinot extra configuration like TlsPort, etc
 */
public class ExtraInstanceConfig {

  private final InstanceConfig _proxy;

  public enum PinotInstanceConfigProperty {
    PINOT_TLS_PORT
  }

  public ExtraInstanceConfig(InstanceConfig proxy) {
    _proxy = proxy;
  }

  public String getTlsPort() {
    return _proxy.getRecord().getSimpleField(PinotInstanceConfigProperty.PINOT_TLS_PORT.toString());
  }

  public void setTlsPort(String tlsPort) {
    _proxy.getRecord().setSimpleField(PinotInstanceConfigProperty.PINOT_TLS_PORT.toString(), tlsPort);
  }

  /**
   * Returns an instance URL from the InstanceConfig. Will set the appropriate protocol and port. Note that the helix
   * participant port will be returned. For the Pinot server this will not correspond to the admin port.
   * Returns null if the URL cannot be constructed.
   */
  public String getComponentUrl() {
    String protocol = null;
    String port = null;
    try {
      if (Integer.parseInt(getTlsPort()) > 0) {
        protocol = CommonConstants.HTTPS_PROTOCOL;
        port = getTlsPort();
      }
    } catch (Exception e) {
      try {
        if (Integer.parseInt(_proxy.getPort()) > 0) {
          protocol = CommonConstants.HTTP_PROTOCOL;
          port = _proxy.getPort();
        }
      } catch (Exception ignored) {
      }
    }
    if (port != null) {
      return String.format("%s://%s:%s", protocol, _proxy.getHostName(), port);
    }
    return null;
  }
}
