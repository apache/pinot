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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.http.client.utils.URIBuilder;


/**
 * RoundRobinURIProvider accept a list of URIs and whether to resolve them into multiple URIs with IP address.
 * If resolveHost = true, it returns a IP address URI in a Round Robin way.
 * If resolveHost = false, then it returns a URI in a Round Robin way.
 */
public class RoundRobinURIProvider {

  private final List<URI> _uris;
  private int _index;

  public RoundRobinURIProvider(List<URI> originalUris, boolean resolveHost)
      throws UnknownHostException, URISyntaxException {
    if (resolveHost) {
      _uris = resolveHostsToIPAddresses(originalUris);
    } else {
      _uris = List.copyOf(originalUris);
    }
    _index = new Random().nextInt(_uris.size());
  }

  public int numAddresses() {
    return _uris.size();
  }

  public URI next() {
    URI result = _uris.get(_index);
    _index = (_index + 1) % _uris.size();
    return result;
  }

  private List<URI> resolveHostToIPAddresses(URI originalUri)
      throws UnknownHostException, URISyntaxException {
    List<URI> resolvedUris = new ArrayList<>();
    String hostName = originalUri.getHost();
    if (InetAddresses.isInetAddress(hostName)) {
      resolvedUris.add(originalUri);
    } else {
      // Resolve host name to IP addresses via DNS
      InetAddress[] addresses = InetAddress.getAllByName(hostName);
      URIBuilder uriBuilder = new URIBuilder(originalUri);
      for (InetAddress address : addresses) {
        String ip = address.getHostAddress();
        resolvedUris.add(uriBuilder.setHost(ip).build());
      }
    }
    return resolvedUris;
  }

  private List<URI> resolveHostsToIPAddresses(List<URI> originalUri)
      throws UnknownHostException, URISyntaxException {
    List<URI> resolvedUris = new ArrayList<>();
    for (URI uri : originalUri) {
      resolvedUris.addAll(resolveHostToIPAddresses(uri));
    }
    return resolvedUris;
  }
}
