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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;


/**
 * Utility for encoding admin client URL path segments. Stateless and thread-safe.
 */
final class PinotAdminPathUtils {
  private PinotAdminPathUtils() {
  }

  static String encodePathSegment(String pathSegment) {
    // URLEncoder applies application/x-www-form-urlencoded rules, where spaces become '+'. For URL path segments
    // spaces must be percent-encoded, so replace '+' with "%20".
    return URLEncoder.encode(pathSegment, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
