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
package org.apache.pinot.broker.api;

import com.google.common.collect.Multimap;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Identity container for HTTP requests with (optional) authorization headers
 */
public class HttpRequesterIdentity extends RequesterIdentity {
  private Multimap<String, String> _httpHeaders;
  private String _endpointUrl;

  public Multimap<String, String> getHttpHeaders() {
    return _httpHeaders;
  }

  public void setHttpHeaders(Multimap<String, String> httpHeaders) {
    _httpHeaders = httpHeaders;
  }

  public String getEndpointUrl() {
    return _endpointUrl;
  }

  public void setEndpointUrl(String endpointUrl) {
    _endpointUrl = endpointUrl;
  }

  /**
   * If reverse proxy is used X-Forwarded-For will be populated
   * If X-Forwarded-For is not present, check if x-real-ip is present
   * Since X-Forwarded-For can contain comma separated list of values, we convert it to ";" delimiter to avoid
   * downstream parsing errors for other fields where "," is being used
   */
  @Override
  public String getClientIp() {
    if (_httpHeaders != null) {
      StringBuilder clientIp = new StringBuilder();
      for (Map.Entry<String, String> entry : _httpHeaders.entries()) {
        String key = entry.getKey();
        String value = entry.getValue();
        if (key.equalsIgnoreCase("x-forwarded-for")) {
          if (value.contains(",")) {
            clientIp.append(String.join(";", value.split(",")));
          } else {
            clientIp.append(value);
          }
        } else if (key.equalsIgnoreCase("x-real-ip")) {
          clientIp.append(value);
        }
      }
      return clientIp.length() == 0 ? CommonConstants.UNKNOWN : clientIp.toString();
    } else {
      return super.getClientIp();
    }
  }
}
