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
package org.apache.pinot.common.minion;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import org.apache.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MinionClientTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionClientTest.class);

  private HttpHandler createHandler(int status, String msg, int sleepTimeMs) {
    return httpExchange -> {
      if (sleepTimeMs > 0) {
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
          LOGGER.warn("Handler interrupted during sleep");
        }
      }
      httpExchange.sendResponseHeaders(status, msg.length());
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(msg.getBytes());
      responseBody.close();
    };
  }

  private HttpServer startServer(int port, String path, HttpHandler handler)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(path, handler);
    new Thread(() -> server.start()).start();
    return server;
  }

  @Test
  public void testTaskSchedule()
      throws IOException, HttpException {
    HttpServer httpServer = startServer(14202, "/tasks/schedule",
        createHandler(200, "{\"SegmentGenerationAndPushTask\":\"Task_SegmentGenerationAndPushTask_1607470525615\"}",
            0));
    MinionClient minionClient = new MinionClient("http://localhost:14202/", null);
    Assert.assertEquals(minionClient.scheduleMinionTasks(null, null).get("SegmentGenerationAndPushTask"),
        "Task_SegmentGenerationAndPushTask_1607470525615");
    httpServer.stop(0);
  }

  @Test
  public void testTasksStates()
      throws IOException, HttpException {
    HttpServer httpServer = startServer(14203, "/tasks/SegmentGenerationAndPushTask/taskstates",
        createHandler(200, "{\"Task_SegmentGenerationAndPushTask_1607470525615\":\"IN_PROGRESS\"}", 0));
    MinionClient minionClient = new MinionClient("http://localhost:14203", null);
    Assert.assertEquals(minionClient.getTasksStates("SegmentGenerationAndPushTask")
        .get("Task_SegmentGenerationAndPushTask_1607470525615"), "IN_PROGRESS");
    httpServer.stop(0);
  }

  @Test
  public void testTaskState()
      throws IOException, HttpException {
    HttpServer httpServer = startServer(14204, "/tasks/task/Task_SegmentGenerationAndPushTask_1607470525615/state",
        createHandler(200, "\"COMPLETED\"", 0));
    MinionClient minionClient = new MinionClient("http://localhost:14204", null);
    Assert.assertEquals(minionClient.getTaskState("Task_SegmentGenerationAndPushTask_1607470525615"), "\"COMPLETED\"");
    httpServer.stop(0);
  }
}
