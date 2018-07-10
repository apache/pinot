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
package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.common.response.ServerInstance;


public class SelectingFutureTest {
  protected static Logger LOGGER = LoggerFactory.getLogger(SelectingFutureTest.class);

  @Test
  /**
   * 3 futures. Happy path. we got response from the first future
   * @throws Exception
   */
  public void testMultiFutureComposite1() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 3;
    List<ServerResponseFuture<String>> futureList = new ArrayList<>();
    String expectedMessage = null;
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureList.add(future);
    }
    SelectingFuture<String> compositeFuture = new SelectingFuture<>("test");
    compositeFuture.start(futureList);
    ResponseCompositeFutureClientRunnerListener runner =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    ResponseCompositeFutureClientRunnerListener listener =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    compositeFuture.addListener(listener, null);
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    executor.execute(runner);
    runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
    Thread.sleep(100);

    // Send response for underlying futures. Key : key_0 first
    for (int i = 0; i < numFutures; i++) {
      String message = "dummy Message_" + i;
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureList.get(i);
      future.onSuccess(message);
    }
    expectedMessage = "dummy Message_0";
    runner.waitForDone();
    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertNull(runner.getError(), "Composite No Error :");
    Assert.assertEquals(runner.getMessage(), expectedMessage, "Response");
    Assert.assertFalse(futureList.get(0).isCancelled(), "First response not cancelled");
    Assert.assertTrue(futureList.get(1).isCancelled(), "Second response cancelled");
    Assert.assertTrue(futureList.get(2).isCancelled(), "Third response cancelled");
    Assert.assertNull(futureList.get(0).getError());
    Assert.assertNull(futureList.get(1).getError());
    Assert.assertNull(futureList.get(2).getError());
    Assert.assertEquals(futureList.get(0).getOne(), runner.getMessage());
    Assert.assertNull(futureList.get(1).get());
    Assert.assertNull(futureList.get(2).get());
    executor.shutdown();
  }

  @Test
  /**
   * 3 futures. we got errors from all the futures
   * @throws Exception
   */
  public void testMultiFutureComposite2() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 3;
    List<ServerResponseFuture<String>> futureList = new ArrayList<>();
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureList.add(future);
    }
    SelectingFuture<String> compositeFuture = new SelectingFuture<>("abc");
    compositeFuture.start(futureList);
    ResponseCompositeFutureClientRunnerListener runner =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    ResponseCompositeFutureClientRunnerListener listener =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    compositeFuture.addListener(listener, null);
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    executor.execute(runner);
    runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
    Thread.sleep(100);

    // Send response for underlying futures. The last error sent is the expected one to show up
    Throwable error = null;
    for (int i = 0; i < numFutures; i++) {
      String message = "dummy Message_" + i;
      error = new Exception(message);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureList.get(i);
      future.onError(error);
    }
    Throwable expectedError = error;
    runner.waitForDone();
    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertNull(runner.getMessage(), "Composite No Message :");
    Assert.assertEquals(runner.getError(), expectedError, "Error");
    Assert.assertFalse(futureList.get(0).isCancelled(), "First response not cancelled");
    Assert.assertFalse(futureList.get(1).isCancelled(), "Second response not cancelled");
    Assert.assertFalse(futureList.get(2).isCancelled(), "Third response not cancelled");
    Assert.assertNotNull(futureList.get(0).getError());
    Assert.assertNotNull(futureList.get(1).getError());
    ServerInstance key2 = new ServerInstance("localhost:2");
    Assert.assertEquals(futureList.get(2).getError().get(key2), runner.getError());
    Assert.assertNull(futureList.get(0).get());
    Assert.assertNull(futureList.get(1).get());
    Assert.assertNull(futureList.get(2).get());
    executor.shutdown();
  }

  @Test
  /**
   * 3 futures. we got errors from first future, response from second future and error from the 3rd future.
   * The composite future should have second future and no error.
   * @throws Exception
   */
  public void testMultiFutureComposite3() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 3;
    List<ServerResponseFuture<String>> futureList = new ArrayList<>();
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureList.add(future);
    }
    SelectingFuture<String> compositeFuture = new SelectingFuture<>("test");
    compositeFuture.start(futureList);
    ResponseCompositeFutureClientRunnerListener runner =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    ResponseCompositeFutureClientRunnerListener listener =
        new ResponseCompositeFutureClientRunnerListener(compositeFuture);
    compositeFuture.addListener(listener, null);
    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    executor.execute(runner);
    runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
    Thread.sleep(100);

    String expectedMessage = null;

    String message = "dummy Message_" + 0;
    // First future gets an error
    Throwable error = new Exception(message);
    AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureList.get(0);
    future.onError(error);
    //Second future gets a response
    message = "dummy Message_" + 1;
    future = (AsyncResponseFuture<String>) futureList.get(1);
    future.onSuccess(message);
    expectedMessage = message;

    // Third future gets a response
    message = "dummy Message_" + 2;
    error = new Exception(message);
    future = (AsyncResponseFuture<String>) futureList.get(2);
    future.onError(error);

    runner.waitForDone();
    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertNull(runner.getError(), "Composite No Error :");
    Assert.assertEquals(runner.getMessage(), expectedMessage, "Response");
    Assert.assertFalse(futureList.get(0).isCancelled(), "First response not cancelled");
    Assert.assertFalse(futureList.get(1).isCancelled(), "Second response not cancelled");
    Assert.assertTrue(futureList.get(2).isCancelled(), "Third response cancelled");
    Assert.assertNotNull(futureList.get(0).getError());
    Assert.assertNull(futureList.get(1).getError());
    Assert.assertNull(futureList.get(2).getError());
    Assert.assertNull(futureList.get(0).get());
    Assert.assertEquals(futureList.get(1).getOne(), runner.getMessage());
    Assert.assertNull(futureList.get(2).get());
    executor.shutdown();
  }

  /**
   * Same class used both as a listener and the one that blocks on get().
   */
  private static class ResponseCompositeFutureClientRunnerListener implements Runnable {
    private boolean _isDone;
    private boolean _isCancelled;
    private String _message;
    private Throwable _error;
    private final SelectingFuture<String> _future;
    private final CountDownLatch _latch = new CountDownLatch(1);
    private final CountDownLatch _endLatch = new CountDownLatch(1);

    public ResponseCompositeFutureClientRunnerListener(SelectingFuture<String> f) {
      _future = f;
    }

    public void waitForAboutToGet() throws InterruptedException {
      _latch.await();
    }

    public void waitForDone() throws InterruptedException {
      _endLatch.await();
    }

    @Override
    public synchronized void run() {
      LOGGER.info("Running Future runner !!");

      String message = null;

      try {
        _latch.countDown();
        message = _future.getOne();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      _isDone = _future.isDone();
      _isCancelled = _future.isCancelled();

      if (null != message) {
        _message = message;
      }
      Map<ServerInstance, Throwable> t = _future.getError();
      if ((null != t) && (!t.isEmpty())) {
        _error = t.values().iterator().next();
      }
      _endLatch.countDown();
      LOGGER.info("End Running Listener !!");
    }

    public boolean isDone() {
      return _isDone;
    }

    public boolean isCancelled() {
      return _isCancelled;
    }

    public String getMessage() {
      return _message;
    }

    public Throwable getError() {
      return _error;
    }

    public SelectingFuture<String> getFuture() {
      return _future;
    }
  }
}
