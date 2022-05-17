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
package org.apache.pinot.spi.auth;

import java.util.Map;


/**
 * Pluggable auth provider interface to augment authentication information in requests issued by pinot.
 *
 * Comes with several default implementation, including noop, static tokens, and token loaded from external urls.
 * The purpose of AuthProvider is enabling dynamic reconfiguration of pinot's internal auth tokens, for example with
 * expiring JWTs and other token rotation mechanisms.
 */
public interface AuthProvider {
  Map<String, Object> getRequestHeaders();

  String getTaskToken();
}
