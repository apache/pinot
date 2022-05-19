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
package org.apache.pinot.common.auth;

import java.util.Collections;
import java.util.Map;
import org.apache.pinot.spi.auth.AuthProvider;


/**
 * Noop auth provider
 */
public class NullAuthProvider implements AuthProvider {
  public NullAuthProvider() {
    // left blank
  }

  public NullAuthProvider(AuthConfig ignore) {
    // left blank
  }

  @Override
  public Map<String, Object> getRequestHeaders() {
    return Collections.emptyMap();
  }

  @Override
  public String getTaskToken() {
    return null;
  }
}
