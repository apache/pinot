/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import java.util.HashMap;
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
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;


public class CompositeFutureTest {
  protected static Logger LOGGER = LoggerFactory.getLogger(CompositeFutureTest.class);

  @Test
  /**
   * Happy path. we got response from all the futures
   * @throws Exception
   */
  public void testMultiFutureComposite1() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 100;
    Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
    Map<ServerInstance, String> expectedMessages = new HashMap<>();
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureMap.put(key, future);
    }
    CompositeFuture<String> compositeFuture =
        new CompositeFuture<String>("test", GatherModeOnError.AND);
    compositeFuture.start(futureMap.values());
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

    // Send response for underlying futures
    for (int i = 0; i < numFutures; i++) {
      String message = "dummy Message_" + i;
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      future.onSuccess(message);
      expectedMessages.put(k, message);
    }

    runner.waitForDone();
    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");

    Map<ServerInstance, String> runnerResponse = runner.getMessage();
    Map<ServerInstance, String> listenerResponse = listener.getMessage();

    for (int i = 0; i < numFutures; i++) {
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertEquals(future.getOne(), expectedMessages.get(k), "Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
      Assert.assertEquals(runnerResponse.get(k), expectedMessages.get(k), "Message_" + i);
      Assert.assertEquals(listenerResponse.get(k), expectedMessages.get(k), "Message_" + i);
    }

    Assert.assertFalse(listener.isCancelled(), "listener Cancelled ?");
    Assert.assertTrue(listener.isDone(), "listener Is Done ? ");
    Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
    executor.shutdown();
  }

  @Test
  /**
   * We got error from all the futures and stoponFirstError is false
   * @throws Exception
   */
  public void testMultiFutureComposite2() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 100;
    Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
    Map<ServerInstance, Exception> expectedErrors = new HashMap<>();
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureMap.put(key, future);
    }
    CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
    compositeFuture.start(futureMap.values());
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

    // Send response for underlying futures
    for (int i = 0; i < numFutures; i++) {
      Exception expectedError = new Exception("error processing_" + i);
      ServerInstance k = new ServerInstance("localhost:" + i);
      ServerResponseFuture<String> future = futureMap.get(k);
      ((AsyncResponseFuture<String>) future).onError(expectedError);
      expectedErrors.put(k, expectedError);
    }

    runner.waitForDone();
    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Response :");

    Map<ServerInstance, Throwable> runnerException = runner.getError();
    Map<ServerInstance, Throwable> listenerException = listener.getError();

    for (int i = 0; i < numFutures; i++) {
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertEquals(future.getError().values().iterator().next(), expectedErrors.get(k), "Error :");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertEquals(runnerException.get(k), expectedErrors.get(k), "Message_" + i);
      Assert.assertEquals(listenerException.get(k), expectedErrors.get(k), "Message_" + i);
    }

    Assert.assertFalse(listener.isCancelled(), "listener Cancelled ?");
    Assert.assertTrue(listener.isDone(), "listener Is Done ? ");
    Assert.assertTrue(listener.getMessage().isEmpty(), "listener No Response :");
    executor.shutdown();
  }

  @Test
  /**
   * Cancelled Future. Future Client calls get() and another listens before cancel().
   * A response and exception arrives after cancel but they should be discarded.
   */
  public void testMultiFutureComposite3() throws Exception {

    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 100;
    int numSuccessFutures = 50;
    Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<String>(key, "");
      futureMap.put(key, future);
    }
    CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
    compositeFuture.start(futureMap.values());
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

    compositeFuture.cancel(false);

    // Send response for some of the underlying futures
    for (int i = 0; i < numSuccessFutures; i++) {
      String message = "dummy Message_" + i;
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      future.onSuccess(message);
    }

    // Send exception for some of the underlying futures
    for (int i = numSuccessFutures; i < numFutures; i++) {
      Exception expectedError = new Exception("error processing_" + i);
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      future.onError(expectedError);
    }

    runner.waitForDone();
    Assert.assertTrue(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Reponse :");
    Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");

    for (int i = 0; i < numFutures; i++) {
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      Assert.assertTrue(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
    }
    Assert.assertTrue(listener.isCancelled(), "listener Cancelled ?");
    Assert.assertTrue(listener.isDone(), "listener Is Done ? ");
    Assert.assertTrue(listener.getMessage().isEmpty(), "listener No Reponse :");
    Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
    executor.shutdown();
  }

  @Test
  /**
   * 100 futures, we get responses from 5 and then get an error. stopOnFirstError = true
   * We expect all the futures to be marked done. the 5 futures will have responses. The next one will have error.
   * Rest will be cancelled.
   * @throws Exception
   */
  public void testMultiFutureComposite4() throws Exception {
    List<ServerInstance> keys = new ArrayList<>();
    int numFutures = 100;
    int numSuccessFutures = 5;
    Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
    Map<ServerInstance, String> expectedMessages = new HashMap<>();

    for (int i = 0; i < numFutures; i++) {
      ServerInstance key = new ServerInstance("localhost:" + i);
      keys.add(key);
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key, "");
      futureMap.put(key, future);
    }
    CompositeFuture<String> compositeFuture =
        new CompositeFuture<>("a", GatherModeOnError.SHORTCIRCUIT_AND); //stopOnError = true
    compositeFuture.start(futureMap.values());
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

    // Send response for underlying futures
    for (int i = 0; i < numSuccessFutures; i++) {
      String message = "dummy Message_" + i;
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      future.onSuccess(message);
      expectedMessages.put(k, message);
    }

    // Send error. This should complete the future/.
    ServerInstance errorKey = new ServerInstance("localhost:" + numSuccessFutures);
    Exception expectedException = new Exception("Exception");
    AsyncResponseFuture<String> f = (AsyncResponseFuture<String>) futureMap.get(errorKey);
    f.onError(expectedException);
    runner.waitForDone();

    Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
    Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
    Assert.assertFalse(listener.isCancelled(), "listener Cancelled ?");
    Assert.assertTrue(listener.isDone(), "listener Is Done ? ");

    Map<ServerInstance, Throwable> runnerException = runner.getError();
    Map<ServerInstance, String> runnerMessages = runner.getMessage();
    Map<ServerInstance, String> listenerMessages = listener.getMessage();
    Map<ServerInstance, Throwable> listenerException = listener.getError();

    for (int i = 0; i < numSuccessFutures; i++) {
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertEquals(future.getOne(), expectedMessages.get(k), "Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
      Assert.assertEquals(runnerMessages.get(k), expectedMessages.get(k), "Message_" + i);
      Assert.assertEquals(listenerMessages.get(k), expectedMessages.get(k), "Message_" + i);
    }

    ServerInstance key1 = new ServerInstance("localhost:" + numSuccessFutures);
    f = (AsyncResponseFuture<String>) futureMap.get(key1);
    Assert.assertFalse(f.isCancelled(), "Cancelled ?");
    Assert.assertTrue(f.isDone(), "Is Done ? ");
    Assert.assertEquals(f.getError().values().iterator().next(), expectedException, "Exception :");
    Assert.assertNull(f.get(), "No Response :");
    Assert.assertEquals(runnerException.get(key1), expectedException, "Exception_" + numSuccessFutures);
    Assert.assertEquals(listenerException.get(key1), expectedException, "Exception_" + numSuccessFutures);

    for (int i = numSuccessFutures + 1; i < numFutures; i++) {
      ServerInstance k = new ServerInstance("localhost:" + i);
      AsyncResponseFuture<String> future = (AsyncResponseFuture<String>) futureMap.get(k);
      Assert.assertTrue(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
      Assert.assertNull(runnerMessages.get(k));
      Assert.assertNull(listenerMessages.get(k));
      Assert.assertNull(runnerException.get(k));
      Assert.assertNull(listenerException.get(k));
    }

    executor.shutdown();
  }

  @Test
  /**
   * Tests Composite future with one underlying future.
   * @throws Exception
   */
  public void testSingleFutureComposite() throws Exception {

    ServerInstance key1 = new ServerInstance("localhost:8080");

    //Cancelled Future. Future Client calls get() and another listens before cancel().
    // A response and exception arrives after cancel but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
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

      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);

      runner.waitForDone();
      Assert.assertTrue(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Reponse :");
      Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");
      Assert.assertTrue(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
      Assert.assertTrue(listener.isCancelled(), "listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "listener Is Done ? ");
      Assert.assertTrue(listener.getMessage().isEmpty(), "listener No Reponse :");
      Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
      executor.shutdown();
    }

    //Cancelled Future. Future Client calls get() and another listens after cancel()
    // A response and exception arrives after cancel but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
      ResponseCompositeFutureClientRunnerListener runner =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);

      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor =
          new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertTrue(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Reponse :");
      Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");
      Assert.assertTrue(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertNull(future.getError(), "No Error :");
      Assert.assertTrue(listener.isCancelled(), "listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "listener Is Done ? ");
      Assert.assertTrue(listener.getMessage().isEmpty(), "listener No Reponse :");
      Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() and another listens before exception
    // A response and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
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

      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);

      runner.waitForDone();
      Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Reponse :");
      Assert.assertEquals(runner.getError().get(key1), expectedError, "Composite Error");
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertEquals(future.getError().values().iterator().next(), expectedError, "Error");
      Assert.assertFalse(listener.isCancelled(), "Listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "Listener Is Done ? ");
      Assert.assertTrue(listener.getMessage().isEmpty(), "Listener No Reponse :");
      Assert.assertEquals(listener.getError().get(key1), expectedError, "Listener Error");
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() and another listens after exception
    // A response and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
      ResponseCompositeFutureClientRunnerListener runner =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      Exception expectedError = new Exception("error processing");

      future.onError(expectedError);
      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor =
          new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getMessage().isEmpty(), "Composite No Reponse :");
      Assert.assertEquals(runner.getError().get(key1), expectedError, "Composite Error");
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertNull(future.get(), "No Reponse :");
      Assert.assertEquals(future.getError().values().iterator().next(), expectedError, "Error");
      Assert.assertFalse(listener.isCancelled(), "Listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "Listener Is Done ? ");
      Assert.assertTrue(listener.getMessage().isEmpty(), "Listener No Reponse :");
      Assert.assertEquals(listener.getError().get(key1), expectedError, "Listener Error");
      executor.shutdown();
    }

    // Get Response. Future Client calls get() and another listens before response
    // An exception and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
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

      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);

      runner.waitForDone();
      Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");
      Assert.assertEquals(runner.getMessage().get(key1), message, "Composite Message");
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertEquals(future.getOne(), message, "Reponse :");
      Assert.assertNull(future.getError(), "No Error");
      Assert.assertFalse(listener.isCancelled(), "Listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "Listener Is Done ? ");
      Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
      Assert.assertEquals(listener.getMessage().get(key1), message, "listener Message");
      executor.shutdown();
    }

    // Get Response. Future Client calls get() and another listens after response
    // An exception and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String> future = new AsyncResponseFuture<>(key1, "");
      Map<ServerInstance, ServerResponseFuture<String>> futureMap = new HashMap<>();
      futureMap.put(key1, future);
      CompositeFuture<String> compositeFuture = new CompositeFuture<>("a", GatherModeOnError.AND);
      compositeFuture.start(futureMap.values());
      ResponseCompositeFutureClientRunnerListener runner =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener =
          new ResponseCompositeFutureClientRunnerListener(compositeFuture);

      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor =
          new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertFalse(runner.isCancelled(), "Composite Cancelled ?");
      Assert.assertTrue(runner.isDone(), "Composite Is Done ? ");
      Assert.assertTrue(runner.getError().isEmpty(), "Composite No Error :");
      Assert.assertEquals(runner.getMessage().get(key1), message, "Composite Message");
      Assert.assertFalse(future.isCancelled(), "Cancelled ?");
      Assert.assertTrue(future.isDone(), "Is Done ? ");
      Assert.assertEquals(future.getOne(), message, "Reponse :");
      Assert.assertNull(future.getError(), "No Error");
      Assert.assertFalse(listener.isCancelled(), "Listener Cancelled ?");
      Assert.assertTrue(listener.isDone(), "Listener Is Done ? ");
      Assert.assertTrue(listener.getError().isEmpty(), "listener No Error :");
      Assert.assertEquals(listener.getMessage().get(key1), message, "listener Message");
      executor.shutdown();
    }
  }

  /**
   * Same class used both as a listener and the one that blocks on get().
   */
  private static class ResponseCompositeFutureClientRunnerListener implements Runnable {
    private boolean _isDone;
    private boolean _isCancelled;
    private Map<ServerInstance, String> _message;
    private Map<ServerInstance, Throwable> _errorMap;
    private final CompositeFuture<String> _future;
    private final CountDownLatch _latch = new CountDownLatch(1);
    private final CountDownLatch _endLatch = new CountDownLatch(1);

    public ResponseCompositeFutureClientRunnerListener(CompositeFuture<String> f) {
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

      Map<ServerInstance, String> message = null;

      try {
        _latch.countDown();
        message = _future.get();
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
      _errorMap = _future.getError();
      _endLatch.countDown();
      LOGGER.info("End Running Listener !!");
    }

    public boolean isDone() {
      return _isDone;
    }

    public boolean isCancelled() {
      return _isCancelled;
    }

    public Map<ServerInstance, String> getMessage() {
      return _message;
    }

    public Map<ServerInstance, Throwable> getError() {
      return _errorMap;
    }

    public CompositeFuture<String> getFuture() {
      return _future;
    }
  }

}
