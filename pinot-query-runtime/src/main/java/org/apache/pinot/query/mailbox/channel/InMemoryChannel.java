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
package org.apache.pinot.query.mailbox.channel;

import java.util.concurrent.ArrayBlockingQueue;


public class InMemoryChannel<T> {
  private final ArrayBlockingQueue<T> _channel;
  private final String _hostname;
  private final int _port;
  private boolean _completed = false;

  public InMemoryChannel(ArrayBlockingQueue<T> channel, String hostname, int port) {
    _channel = channel;
    _hostname = hostname;
    _port = port;
  }

  public ArrayBlockingQueue<T> getChannel() {
    return _channel;
  }

  public void complete() {
    _completed = true;
  }

  public boolean isCompleted() {
    return _completed;
  }
}
