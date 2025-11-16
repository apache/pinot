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
package org.apache.pinot.connector.spark.common

/** Network-related helpers for parsing and normalization. */
private[pinot] object NetUtils {

  /**
   * Parse a host[:port] string, returning (host, portString) where port defaults by security.
   *
   * @param hostPort input like "example.com:8080" or "example.com"
   * @param secure if true, default port is 443, else 80
   */
  def parseHostPort(hostPort: String, secure: Boolean): (String, String) = {
    hostPort.split(":", 2) match {
      case Array(h, p) if p.forall(_.isDigit) =>
        (h, p)
      case Array(h) =>
        val defaultPort = if (secure) "443" else "80"
        (h, defaultPort)
      case _ =>
        throw new IllegalArgumentException(s"Invalid host:port: '$hostPort'")
    }
  }
}


