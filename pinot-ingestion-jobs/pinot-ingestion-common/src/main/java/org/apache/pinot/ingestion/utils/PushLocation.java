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
package org.apache.pinot.ingestion.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class PushLocation implements Serializable {
  private final String _host;
  private final int _port;

  public PushLocation(String host, int port) {
    _host = host;
    _port = port;
  }

  public static List<PushLocation> getPushLocations(String[] hosts, int port) {
    List<PushLocation> pushLocations = new ArrayList<>(hosts.length);
    for (String host : hosts) {
      pushLocations.add(new PushLocation(host, port));
    }
    return pushLocations;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  @Override
  public String toString() {
    return _host + ":" + _port;
  }
}
