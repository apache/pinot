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
package org.apache.pinot.broker.grpc;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Map;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.spi.utils.CommonConstants;


public class GrpcRequesterIdentity extends RequesterIdentity {
  private Multimap<String, String> _metadata;

  public static GrpcRequesterIdentity fromRequest(Server.ServerRequest request) {
    Multimap<String, String> metadata = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : request.getMetadataMap().entrySet()) {
      metadata.put(entry.getKey(), entry.getValue());
    }
    GrpcRequesterIdentity identity = new GrpcRequesterIdentity();
    identity.setMetadata(metadata);
    return identity;
  }

  public static GrpcRequesterIdentity fromRequest(Broker.BrokerRequest request) {
    Multimap<String, String> metadata = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : request.getMetadataMap().entrySet()) {
      metadata.put(entry.getKey(), entry.getValue());
    }
    GrpcRequesterIdentity identity = new GrpcRequesterIdentity();
    identity.setMetadata(metadata);
    return identity;
  }

  public Multimap<String, String> getMetadata() {
    return _metadata;
  }

  public void setMetadata(Multimap<String, String> metadata) {
    _metadata = metadata;
  }

  /**
   * Following the same logic of
   * {@link org.apache.pinot.broker.api.HttpRequesterIdentity} to get client IP
   * If reverse proxy is used X-Forwarded-For will be populated
   * If X-Forwarded-For is not present, check if x-real-ip is present
   * Since X-Forwarded-For can contain comma separated list of values,
   * we convert it to ";" delimiter to avoid
   * downstream parsing errors for other fields where "," is being used
   */
  @Override
  public String getClientIp() {
    if (_metadata != null) {
      StringBuilder clientIp = new StringBuilder();
      for (Map.Entry<String, String> entry : _metadata.entries()) {
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
