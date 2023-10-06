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
package org.apache.pinot.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;


/**
 * TLS Protocols enabled for AsyncHttpClient
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TlsProtocols {
    private final List<String> _enabledProtocols;

    public List<String> getEnabledProtocols() {
        if (_enabledProtocols != null) {
            return _enabledProtocols;
        }
        return Collections.emptyList();
    }

    public static TlsProtocols defaultProtocols(boolean tlsV10Enabled) {
        List<String> enabledProtocols = new ArrayList<>();
        enabledProtocols.add("TLSv1.3");
        enabledProtocols.add("TLSv1.2");
        enabledProtocols.add("TLSv1.1");
        if (tlsV10Enabled) {
            enabledProtocols.add("TLSv1.0");
        }
        return new TlsProtocols(enabledProtocols);
    }
}
