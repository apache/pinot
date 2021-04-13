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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.testng.annotations.Test;


public class NetUtilTest {

  @Test
  public void testNetUtil()
      throws SocketException, UnknownHostException {
    System.out.println("NetUtil.getHostAddress(): " + NetUtil.getHostAddress());
    System.out.println("InetAddress.getLocalHost().getHostName(): " + InetAddress.getLocalHost().getHostName());
    System.out.println("InetAddress.getLocalHost().getHostAddress(): " + InetAddress.getLocalHost().getHostAddress());
    System.out.println("InetAddress.getLocalHost().getCanonicalHostName(): " + InetAddress.getLocalHost().getCanonicalHostName());
    System.out.println("InetAddress.getLoopbackAddress().getHostName(): " + InetAddress.getLoopbackAddress().getHostName());
    System.out.println("InetAddress.getLoopbackAddress().getHostAddress(): " + InetAddress.getLoopbackAddress().getHostAddress());
    System.out.println("InetAddress.getLoopbackAddress().getCanonicalHostName(): " + InetAddress.getLoopbackAddress().getCanonicalHostName());
    System.out.println("io.netty.util.NetUtil.LOCALHOST.getHostName(): " + io.netty.util.NetUtil.LOCALHOST.getHostName());
    System.out.println("io.netty.util.NetUtil.LOCALHOST.getHostAddress(): " + io.netty.util.NetUtil.LOCALHOST.getHostAddress());
    System.out.println("io.netty.util.NetUtil.LOCALHOST.getCanonicalHostName(): " + io.netty.util.NetUtil.LOCALHOST.getCanonicalHostName());
    System.out.println("new InetSocketAddress(\"localhost\", 9000).getHostName(): " + new InetSocketAddress("localhost", 9000).getHostName());
    System.out.println("new InetSocketAddress(\"localhost\", 9000).getHostString(): " + new InetSocketAddress("localhost", 9000).getHostString());
    System.out.println("new InetSocketAddress(\"127.0.0.1\", 9000).getHostName(): " + new InetSocketAddress("127.0.0.1", 9000).getHostName());
    System.out.println("new InetSocketAddress(\"127.0.0.1\", 9000).getHostString(): " + new InetSocketAddress("127.0.0.1", 9000).getHostString());
    System.out.println("new InetSocketAddress(NetUtil.getHostAddress(), 9000).getHostName(): " + new InetSocketAddress(NetUtil.getHostAddress(), 9000).getHostName());
    System.out.println("new InetSocketAddress(NetUtil.getHostAddress(), 9000).getHostString(): " + new InetSocketAddress(NetUtil.getHostAddress(), 9000).getHostString());
  }
}
