package com.linkedin.pinot.transport.common;

import java.net.InetAddress;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.junit.Test;

public class TestServerInstance extends TestCase {

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  @Test
  public void testServerInstance() throws Exception
  {
    // Same local hostname and port
    {
      ServerInstance instance1 = new ServerInstance("localhost", 8080);
      ServerInstance instance2 = new ServerInstance("localhost", 8080);
      System.out.println("Instance 1 :" + instance1);
      System.out.println("Instance 2 :" + instance2);
      Assert.assertEquals("Localhost server-instances with same port", instance1, instance2);
    }

    // Same  hostname and port
    {
      ServerInstance instance1 = new ServerInstance("test-host", 8080);
      ServerInstance instance2 = new ServerInstance("test-host", 8080);
      System.out.println("Instance 1 :" + instance1);
      System.out.println("Instance 2 :" + instance2);
      Assert.assertEquals("Localhost server-instances with same port", instance1, instance2);
    }
    // same hostname but different port
    {
      ServerInstance instance1 = new ServerInstance("localhost", 8081);
      ServerInstance instance2 = new ServerInstance("localhost", 8082);
      System.out.println("Instance 1 :" + instance1);
      System.out.println("Instance 2 :" + instance2);
      Assert.assertFalse("Localhost server-instances with same port", instance1.equals(instance2));
    }

    // same port but different host
    {
      ServerInstance instance1 = new ServerInstance("abcd", 8080);
      ServerInstance instance2 = new ServerInstance("abce", 8080);
      System.out.println("Instance 1 :" + instance1);
      System.out.println("Instance 2 :" + instance2);
      Assert.assertFalse("Localhost server-instances with same port", instance1.equals(instance2));
    }

    // Test getIpAddress
    {
      InetAddress ipAddr = InetAddress.getByName("127.0.0.1");

      ServerInstance instance1 = new ServerInstance("127.0.0.1", 8080);
      Assert.assertEquals("Port check", 8080, instance1.getPort());
      Assert.assertEquals("Hostname check", 8080, instance1.getPort());
      Assert.assertEquals("Host check", ipAddr.getHostName(), instance1.getHostname());
      Assert.assertEquals("IP check", ipAddr, instance1.getIpAddress());
    }
  }
}
