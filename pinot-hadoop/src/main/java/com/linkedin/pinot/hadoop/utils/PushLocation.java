package com.linkedin.pinot.hadoop.utils;

public class PushLocation {
  private final String _host;
  private final int _port;

  private PushLocation(PushLocationBuilder pushLocationBuilder) {
    _host = pushLocationBuilder._host;
    _port = pushLocationBuilder._port;
  }

  public String getHost() {
    return _host;
  }

  public int getPort() {
    return _port;
  }

  public static class PushLocationBuilder{
    private String _host;
    private int _port;

    public PushLocationBuilder() {
    }

    public PushLocationBuilder setHost(String host) {
      _host = host;
      return this;
    }

    public PushLocationBuilder setPort(int port) {
      _port = port;
      return this;
    }

    public PushLocation build() {
      return new PushLocation(this);
    }
  }
}
