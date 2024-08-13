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
package org.apache.pinot.spi.utils;

import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import org.mockito.MockedConstruction;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class NetUtilsTest {
  private static final String LOCAL_ADDRESS_IPV4 = "172.16.184.0";
  private static final String LOCAL_ADDRESS_IPV6 = "2001:db8::1";

  private enum NetworkEnv {
    IPV4, IPV6, DUAL_STACK
  }

  /**
   * Initialize the mock DatagramSocket constructor with the given mock InetAddress and network environment.
   */
  private static MockedConstruction.MockInitializer<DatagramSocket> initDatagramSocket(InetAddress mockInetAddress,
      NetworkEnv networkEnv) {
    return (mockDatagramSocket, context) -> {
      when(mockDatagramSocket.getLocalAddress()).thenReturn(mockInetAddress);
      switch (networkEnv) {
        case IPV4:
          // IPv6 address is not available
          doThrow(new java.io.UncheckedIOException(new java.net.NoRouteToHostException())).when(mockDatagramSocket)
              .connect(isA(Inet6Address.class), anyInt());
          break;
        case IPV6:
          doThrow(new java.io.UncheckedIOException(new java.net.SocketException())).when(mockDatagramSocket)
              .connect(isA(Inet4Address.class), anyInt());
          break;
        case DUAL_STACK:
          doNothing().when(mockDatagramSocket).connect(isA(InetAddress.class), anyInt());
          break;
        default:
          throw new IllegalArgumentException("Invalid network environment: " + networkEnv);
      }
    };
  }

  @BeforeMethod
  public void setUp() {
    System.clearProperty("java.net.preferIPv6Addresses");
  }

  @Test(description = "Test getHostAddress with no preferIPv6Addresses in IPv4 only environment")
  public void testGetHostAddressIPv4Env() {
    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV4);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.IPV4))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV4, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(1)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }

  @Test(description = "Test getHostAddress with preferIPv6Addresses=true in IPv4 only environment")
  public void testGetHostAddressIPv4EnvIPv6Preferred() {
    System.setProperty("java.net.preferIPv6Addresses", "true");

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV4);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.IPV4))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV4, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(2)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }

  @Test(description = "Test getHostAddress with no preferIPv6Addresses in dual stack environment")
  public void testGetHostAddressDualStackEnv() {
    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV4);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.DUAL_STACK))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV4, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(1)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }

  @Test(description = "Test getHostAddress with preferIPv6Addresses=true in dual stack environment")
  public void testGetHostAddressDualStackEnvIPv6Preferred() {
    System.setProperty("java.net.preferIPv6Addresses", "true");

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV6);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.DUAL_STACK))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV6, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(1)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }

  @Test(description = "Test getHostAddress with no preferIPv6Addresses in IPv6 only environment")
  public void testGetHostAddressIPv6Env() {
    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV6);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.IPV6))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV6, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(2)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }

  @Test(description = "Test getHostAddress with preferIPv6Addresses=true in IPv6 only environment")
  public void testGetHostAddressIPv6EnvIPv6Preferred() {
    System.setProperty("java.net.preferIPv6Addresses", "true");

    InetAddress mockInetAddress = mock(InetAddress.class);
    when(mockInetAddress.isAnyLocalAddress()).thenReturn(false);
    when(mockInetAddress.getHostAddress()).thenReturn(LOCAL_ADDRESS_IPV6);

    try (MockedConstruction<DatagramSocket> mockedConstructionDatagramSocket = mockConstruction(DatagramSocket.class,
        initDatagramSocket(mockInetAddress, NetworkEnv.IPV6))) {
      String hostAddress = NetUtils.getHostAddress();
      DatagramSocket mockDatagramSocket = mockedConstructionDatagramSocket.constructed().get(0);

      assertEquals(LOCAL_ADDRESS_IPV6, hostAddress);
      assertEquals(1, mockedConstructionDatagramSocket.constructed().size());
      verify(mockDatagramSocket, times(1)).connect(any(), anyInt());
    } catch (SocketException | UnknownHostException e) {
      Assert.fail("Should not throw: " + e.getMessage());
    }
  }
}
