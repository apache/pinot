package com.linkedin.thirdeye;

import java.net.InetSocketAddress;

public class ThirdEyeClientFactory
{
  public static ThirdEyeClient createDefault(InetSocketAddress address)
  {
    return new ThirdEyeClientJerseyImpl(address);
  }
}
