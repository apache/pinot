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
package org.apache.pinot.common.utils;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;
import org.apache.http.client.utils.URIBuilder;


/**
 * RoundRobinURIProvider accept a URI, try to resolve it into multiple URIs with IP address, and return a IP address URI
 * in a Round Robin way.
 */
public class RoundRobinURIProvider {

  private final URI[] _uris;
  private int _index;

  public RoundRobinURIProvider(URI originalUri)
      throws UnknownHostException, URISyntaxException {
    String hostName = originalUri.getHost();
    if (InetAddresses.isInetAddress(hostName)) {
      _uris = new URI[]{originalUri};
    } else {
      // Resolve host name to IP addresses via DNS
      InetAddress[] addresses = InetAddress.getAllByName(hostName);
      _uris = new URI[addresses.length];
      URIBuilder uriBuilder = new URIBuilder(originalUri);
      for (int i = 0; i < addresses.length; i++) {
        String ip = addresses[i].getHostAddress();
        _uris[i] = uriBuilder.setHost(ip).build();
      }
    }
    _index = new Random().nextInt(_uris.length);
  }

  public RoundRobinURIProvider(List<URI> uris) {
    _uris = new URI[uris.size()];
    for (int i = 0; i < uris.size(); i++) {
      _uris[i] = uris.get(i);
    }
    _index = new Random().nextInt(_uris.length);
  }

  public int numAddresses() {
    return _uris.length;
  }

  public URI next() {
    URI result = _uris[_index];
    _index = (_index + 1) % _uris.length;
    return result;
  }
}
