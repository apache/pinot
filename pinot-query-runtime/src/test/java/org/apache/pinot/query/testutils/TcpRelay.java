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
package org.apache.pinot.query.testutils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/// A TCP relay on a loopback port that forwards every connection to a target port, which a test can change.
///
/// Tests use it as an IP address that moves from one server to another: [#switchTo] drops the connections to the old
/// target, as when that server goes away, and sends new connections to the new one, as when another server takes the
/// address over. The relay keeps its own port for its whole life, so no other process can take the address in between.
public class TcpRelay implements Closeable {
  private final ServerSocket _listener;
  private final ExecutorService _executor = Executors.newCachedThreadPool();
  private final List<Socket> _sockets = new CopyOnWriteArrayList<>();
  private volatile int _targetPort;
  private volatile boolean _closed;

  public TcpRelay(int targetPort)
      throws IOException {
    _targetPort = targetPort;
    _listener = new ServerSocket();
    _listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    _executor.submit(this::acceptLoop);
  }

  public int getPort() {
    return _listener.getLocalPort();
  }

  /// Closes every connection relayed so far, and relays new connections to `targetPort`.
  public void switchTo(int targetPort) {
    _targetPort = targetPort;
    closeSockets();
  }

  private void acceptLoop() {
    while (!_closed) {
      Socket downstream;
      try {
        downstream = _listener.accept();
      } catch (IOException e) {
        return;
      }
      try {
        Socket upstream = new Socket(InetAddress.getLoopbackAddress(), _targetPort);
        _sockets.add(downstream);
        _sockets.add(upstream);
        _executor.submit(() -> pump(downstream, upstream));
        _executor.submit(() -> pump(upstream, downstream));
      } catch (IOException e) {
        // Nothing listens at the target: refuse the connection, as a closed port would
        closeQuietly(downstream);
      }
    }
  }

  private void pump(Socket from, Socket to) {
    byte[] buffer = new byte[8192];
    try {
      InputStream in = from.getInputStream();
      OutputStream out = to.getOutputStream();
      int read;
      while ((read = in.read(buffer)) >= 0) {
        out.write(buffer, 0, read);
        out.flush();
      }
    } catch (IOException e) {
      // Either side closed the connection
    } finally {
      closeQuietly(from);
      closeQuietly(to);
    }
  }

  private void closeSockets() {
    for (Socket socket : _sockets) {
      closeQuietly(socket);
    }
    _sockets.clear();
  }

  private static void closeQuietly(Socket socket) {
    try {
      socket.close();
    } catch (IOException e) {
      // Best effort
    }
  }

  @Override
  public void close()
      throws IOException {
    _closed = true;
    _listener.close();
    closeSockets();
    _executor.shutdownNow();
  }
}
