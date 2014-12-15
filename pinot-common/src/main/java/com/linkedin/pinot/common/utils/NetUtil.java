package com.linkedin.pinot.common.utils;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;


public class NetUtil {
  private static final String DUMMY_OUT_IP = "74.125.224.0";

  /**
   * Get the ip address of local host.
   */
  public static String getHostAddress() throws SocketException, UnknownHostException {
    DatagramSocket ds = new DatagramSocket();
    ds.connect(InetAddress.getByName(DUMMY_OUT_IP), 80);
    InetAddress localAddress = ds.getLocalAddress();
    if (localAddress.getHostAddress().equals("0.0.0.0")) {
      localAddress = InetAddress.getLocalHost();
    }
    return localAddress.getHostAddress();
  }
}
