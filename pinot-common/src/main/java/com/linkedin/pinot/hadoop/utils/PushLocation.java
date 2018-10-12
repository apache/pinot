/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  @Override
  public String toString() {
    return _host + ":" + _port;
  }
}
