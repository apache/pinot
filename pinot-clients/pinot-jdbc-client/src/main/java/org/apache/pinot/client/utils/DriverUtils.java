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
package org.apache.pinot.client.utils;

import com.ning.http.client.uri.Uri;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DriverUtils {
  public static final String SCHEME = "jdbc";
  public static final String DRIVER = "pinot";
  public static final Logger LOG = LoggerFactory.getLogger(DriverUtils.class);

  private DriverUtils() {
  }

  public static List<String> getBrokersFromURL(String url) {
    if (url.toLowerCase().startsWith("jdbc:")) {
      url = url.substring(5);
    }
    URI uri = URI.create(url);
    return getBrokersFromURI(uri);
  }

  public static List<String> getBrokersFromURI(URI uri) {
    String brokerUrl = String.format("%s:%d", uri.getHost(), uri.getPort());
    List<String> brokerList = Collections.singletonList(brokerUrl);
    return brokerList;
  }

  public static String getURIFromBrokers(List<String> brokers) {
    try {
      String broker = brokers.get(0);
      String[] hostPort = broker.split(":");
      URI uri = new URI(SCHEME + ":" + DRIVER, hostPort[0], hostPort[1]);
      return uri.toString();
    } catch (Exception e) {
      LOG.warn("Broker list is either empty or has incorrect format", e);
    }
    return "";
  }
}
