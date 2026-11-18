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
package org.apache.pinot.controller.helix;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.pinot.spi.utils.NetUtils;


/// An immutable, thread-safe range of test listener ports with an inclusive start and exclusive end. Searches never
/// cross the range boundary. Probing does not reserve a socket, so callers must coordinate their allocation cursors
/// and the subsequent listener binds; concurrent callers can observe the same available port.
public final class TestPortRange {
  private final String _component;
  private final int _start;
  private final int _end;

  public TestPortRange(String component, int start, int end) {
    _component = Objects.requireNonNull(component);
    Preconditions.checkArgument(start > 0 && start < end && end <= 65536,
        "Invalid port range for %s: [%s, %s); expected 1 <= start < end <= 65536", component, start, end);
    _start = start;
    _end = end;
  }

  public int getStart() {
    return _start;
  }

  public int findOpenPort(int startPort) {
    Preconditions.checkArgument(startPort >= _start,
        "Starting port %s is below the %s port range [%s, %s)", startPort, _component, _start, _end);
    for (int port = startPort; port < _end; port++) {
      if (NetUtils.available(port)) {
        return port;
      }
    }
    throw new IllegalStateException("No available port for " + _component + " in range [" + _start + ", " + _end
        + ") starting at " + startPort);
  }
}
