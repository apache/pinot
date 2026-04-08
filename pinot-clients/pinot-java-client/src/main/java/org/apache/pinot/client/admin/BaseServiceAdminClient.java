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
package org.apache.pinot.client.admin;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class for controller admin clients backed by a shared {@link PinotAdminTransport}.
 * Implementations are thread-safe as long as the parent {@link PinotAdminClient} remains open.
 */
abstract class BaseServiceAdminClient {
  protected final PinotAdminTransport _transport;
  protected final String _controllerAddress;
  protected final Map<String, String> _headers;

  protected BaseServiceAdminClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  protected Map<String, String> mergeHeaders(@Nullable Map<String, String> headers) {
    if ((_headers == null || _headers.isEmpty()) && (headers == null || headers.isEmpty())) {
      return Map.of();
    }
    Map<String, String> merged = new HashMap<>();
    if (_headers != null) {
      merged.putAll(_headers);
    }
    if (headers != null) {
      merged.putAll(headers);
    }
    return merged;
  }
}
