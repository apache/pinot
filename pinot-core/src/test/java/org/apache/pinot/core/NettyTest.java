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
package org.apache.pinot.core;

import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.handler.ssl.OpenSsl;
import org.apache.pinot.core.util.OsCheck;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class NettyTest {
  private static void requiredOs(final OsCheck.OSType requiredOsType) {
    if (OsCheck.getOperatingSystemType() != requiredOsType) {
      throw new SkipException("skipping test: " + requiredOsType + " != " + OsCheck.getOperatingSystemType());
    }
  }

  @Test
  public void epollIsAvailableOnLinux() {
    requiredOs(OsCheck.OSType.Linux);
    Epoll.ensureAvailability();
    assertTrue(Epoll.isAvailable());
  }

  @Test
  public void kqueueIsAvailableOnMac() {
    requiredOs(OsCheck.OSType.MacOS);
    KQueue.ensureAvailability();
    assertTrue(KQueue.isAvailable());
  }

  @Test
  public void boringSslIsAvailable() {
    OpenSsl.ensureAvailability();
    assertTrue(OpenSsl.isAvailable());
    assertEquals(OpenSsl.versionString(), "BoringSSL");
  }
}
