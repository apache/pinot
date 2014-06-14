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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.transport.common.ConjunctiveCompositeFuture.GatherModeOnError;

public class TestConjunctiveCompositeFuture {
  protected static Logger LOG = LoggerFactory.getLogger(TestConjunctiveCompositeFuture.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  @Test
  /**
   * Happy path. we got response from all the futures
   * @throws Exception
   */
  public void testMultiFutureComposite1() throws Exception {
    List<String> keys = new ArrayList<String>();
    int numFutures = 100;
    Map<String, AsyncResponseFuture<String, String>> futureMap =
        new HashMap<String, AsyncResponseFuture<String, String>>();
    Map<String, String> expectedMessages = new HashMap<String, String>();
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureMap.put(key, future);
    }
    ConjunctiveCompositeFuture<String, String> compositeFuture =
        new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
    compositeFuture.start(futureMap);
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
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      future.onSuccess(message);
      expectedMessages.put(k, message);
    }

    runner.waitForDone();
    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());

    Map<String, String> runnerResponse = runner.getMessage();
    Map<String, String> listenerResponse = listener.getMessage();

    for (int i = 0; i < numFutures; i++) {
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ", future.isDone());
      Assert.assertEquals("Reponse :", expectedMessages.get(k), future.get());
      Assert.assertNull("No Error :", future.getError());
      Assert.assertEquals("Message_" + i, expectedMessages.get(k), runnerResponse.get(k));
      Assert.assertEquals("Message_" + i, expectedMessages.get(k), listenerResponse.get(k));
    }

    Assert.assertFalse("listener Cancelled ?", listener.isCancelled());
    Assert.assertTrue("listener Is Done ? ", listener.isDone());
    Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
    executor.shutdown();
  }


  @Test
  /**
   * We got error from all the futures and stoponFirstError is false
   * @throws Exception
   */
  public void testMultiFutureComposite2() throws Exception {
    List<String> keys = new ArrayList<String>();
    int numFutures = 100;
    Map<String, AsyncResponseFuture<String, String>> futureMap =
        new HashMap<String, AsyncResponseFuture<String, String>>();
    Map<String, Exception> expectedErrors = new HashMap<String, Exception>();
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureMap.put(key, future);
    }
    ConjunctiveCompositeFuture<String, String> compositeFuture =
        new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
    compositeFuture.start(futureMap);
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
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      future.onError(expectedError);
      expectedErrors.put(k, expectedError);
    }

    runner.waitForDone();
    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertTrue("Composite No Response :", runner.getMessage().isEmpty());

    Map<String, Throwable> runnerException = runner.getError();
    Map<String, Throwable> listenerException = listener.getError();

    for (int i = 0; i < numFutures; i++) {
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ", future.isDone());
      Assert.assertEquals("Error :", expectedErrors.get(k), future.getError());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertEquals("Message_" + i, expectedErrors.get(k), runnerException.get(k));
      Assert.assertEquals("Message_" + i, expectedErrors.get(k), listenerException.get(k));
    }

    Assert.assertFalse("listener Cancelled ?", listener.isCancelled());
    Assert.assertTrue("listener Is Done ? ", listener.isDone());
    Assert.assertTrue("listener No Response :", listener.getMessage().isEmpty());
    executor.shutdown();
  }

  @Test
  /**
   * Cancelled Future. Future Client calls get() and another listens before cancel().
   * A response and exception arrives after cancel but they should be discarded.
   */
  public void testMultiFutureComposite3() throws Exception {

    List<String> keys = new ArrayList<String>();
    int numFutures = 100;
    int numSuccessFutures = 50;
    Map<String, AsyncResponseFuture<String, String>> futureMap =
        new HashMap<String, AsyncResponseFuture<String, String>>();
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureMap.put(key, future);
    }
    ConjunctiveCompositeFuture<String, String> compositeFuture =
        new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
    compositeFuture.start(futureMap);
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
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      future.onSuccess(message);
    }

    // Send exception for some of the underlying futures
    for (int i = numSuccessFutures; i < numFutures; i++) {
      Exception expectedError = new Exception("error processing_" + i);
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      future.onError(expectedError);
    }

    runner.waitForDone();
    Assert.assertTrue("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertTrue("Composite No Reponse :", runner.getMessage().isEmpty());
    Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());

    for (int i = 0; i < numFutures; i++) {
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      Assert.assertTrue("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ", future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertNull("No Error :", future.getError());
    }
    Assert.assertTrue("listener Cancelled ?", listener.isCancelled());
    Assert.assertTrue("listener Is Done ? ", listener.isDone());
    Assert.assertTrue("listener No Reponse :", listener.getMessage().isEmpty());
    Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
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
    List<String> keys = new ArrayList<String>();
    int numFutures = 100;
    int numSuccessFutures = 5;
    Map<String, AsyncResponseFuture<String, String>> futureMap =
        new HashMap<String, AsyncResponseFuture<String, String>>();
    Map<String, String> expectedMessages = new HashMap<String, String>();

    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureMap.put(key, future);
    }
    ConjunctiveCompositeFuture<String, String> compositeFuture =
        new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.SHORTCIRCUIT_AND); //stopOnError = true
    compositeFuture.start(futureMap);
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
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      future.onSuccess(message);
      expectedMessages.put(k, message);
    }

    // Send error. This should complete the future/.
    String errorKey =  "key_" + numSuccessFutures;
    Exception expectedException = new Exception("Exception");
    AsyncResponseFuture<String, String> f = futureMap.get(errorKey);
    f.onError(expectedException);
    runner.waitForDone();

    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertFalse("listener Cancelled ?", listener.isCancelled());
    Assert.assertTrue("listener Is Done ? ", listener.isDone());

    Map<String, Throwable> runnerException = runner.getError();
    Map<String, String> runnerMessages = runner.getMessage();
    Map<String, String> listenerMessages = listener.getMessage();
    Map<String, Throwable> listenerException = listener.getError();


    for (int i = 0; i < numSuccessFutures; i++) {
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ", future.isDone());
      Assert.assertEquals("Reponse :", expectedMessages.get(k), future.get());
      Assert.assertNull("No Error :", future.getError());
      Assert.assertEquals("Message_" + i, expectedMessages.get(k), runnerMessages.get(k));
      Assert.assertEquals("Message_" + i, expectedMessages.get(k), listenerMessages.get(k));
    }

    String key1 = "key_" + numSuccessFutures;
    f = futureMap.get(key1);
    Assert.assertFalse("Cancelled ?", f.isCancelled());
    Assert.assertTrue("Is Done ? ", f.isDone());
    Assert.assertEquals("Exception :", expectedException, f.getError());
    Assert.assertNull("No Response :", f.get());
    Assert.assertEquals("Exception_" + numSuccessFutures, expectedException, runnerException.get(key1));
    Assert.assertEquals("Exception_" + numSuccessFutures, expectedException, listenerException.get(key1));

    for (int i = numSuccessFutures+1; i < numFutures; i++) {
      String k = "key_" + i;
      AsyncResponseFuture<String, String> future = futureMap.get(k);
      Assert.assertTrue("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ", future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertNull("No Error :", future.getError());
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

    String key1 = "localhost:8080";

    //Cancelled Future. Future Client calls get() and another listens before cancel().
    // A response and exception arrives after cancel but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);

      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);

      runner.waitForDone();
      Assert.assertTrue("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Reponse :", runner.getMessage().isEmpty());
      Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());
      Assert.assertTrue("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertNull("No Error :", future.getError());
      Assert.assertTrue("listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("listener Is Done ? ",listener.isDone());
      Assert.assertTrue("listener No Reponse :", listener.getMessage().isEmpty());
      Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
      executor.shutdown();
    }

    //Cancelled Future. Future Client calls get() and another listens after cancel()
    // A response and exception arrives after cancel but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);

      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertTrue("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Reponse :", runner.getMessage().isEmpty());
      Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());
      Assert.assertTrue("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertNull("No Error :", future.getError());
      Assert.assertTrue("listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("listener Is Done ? ",listener.isDone());
      Assert.assertTrue("listener No Reponse :", listener.getMessage().isEmpty());
      Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() and another listens before exception
    // A response and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);

      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);

      runner.waitForDone();
      Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Reponse :", runner.getMessage().isEmpty());
      Assert.assertEquals("Composite Error", expectedError, runner.getError().get(key1));
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertEquals("Error", expectedError, future.getError());
      Assert.assertFalse("Listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("Listener Is Done ? ",listener.isDone());
      Assert.assertTrue("Listener No Reponse :", listener.getMessage().isEmpty());
      Assert.assertEquals("Listener Error", expectedError, listener.getError().get(key1));
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() and another listens after exception
    // A response and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      Exception expectedError = new Exception("error processing");

      future.onError(expectedError);
      compositeFuture.cancel(false);
      String message = "dummy Message";
      future.onSuccess(message);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Reponse :", runner.getMessage().isEmpty());
      Assert.assertEquals("Composite Error", expectedError, runner.getError().get(key1));
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertNull("No Reponse :", future.get());
      Assert.assertEquals("Error", expectedError, future.getError());
      Assert.assertFalse("Listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("Listener Is Done ? ",listener.isDone());
      Assert.assertTrue("Listener No Reponse :", listener.getMessage().isEmpty());
      Assert.assertEquals("Listener Error", expectedError, listener.getError().get(key1));
      executor.shutdown();
    }

    // Get Response. Future Client calls get() and another listens before response
    // An exception and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);

      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);

      runner.waitForDone();
      Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());
      Assert.assertEquals("Composite Message", message, runner.getMessage().get(key1));
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertEquals("Reponse :", message, future.get());
      Assert.assertNull("No Error", future.getError());
      Assert.assertFalse("Listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("Listener Is Done ? ",listener.isDone());
      Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
      Assert.assertEquals("listener Message", message, listener.getMessage().get(key1));
      executor.shutdown();
    }

    // Get Response. Future Client calls get() and another listens after response
    // An exception and cancellation arrives after exception but they should be discarded.
    {
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key1);
      Map<String, AsyncResponseFuture<String, String>> futureMap = new HashMap<String, AsyncResponseFuture<String, String>>();
      futureMap.put(key1, future);
      ConjunctiveCompositeFuture<String, String> compositeFuture = new ConjunctiveCompositeFuture<String, String>(GatherModeOnError.AND);
      compositeFuture.start(futureMap);
      ResponseCompositeFutureClientRunnerListener runner = new ResponseCompositeFutureClientRunnerListener(compositeFuture);
      ResponseCompositeFutureClientRunnerListener listener = new ResponseCompositeFutureClientRunnerListener(compositeFuture);

      String message = "dummy Message";
      future.onSuccess(message);
      Exception expectedError = new Exception("error processing");
      future.onError(expectedError);
      compositeFuture.cancel(false);

      compositeFuture.addListener(listener, null);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Composite Is Done ? ",runner.isDone());
      Assert.assertTrue("Composite No Error :", runner.getError().isEmpty());
      Assert.assertEquals("Composite Message", message, runner.getMessage().get(key1));
      Assert.assertFalse("Cancelled ?", future.isCancelled());
      Assert.assertTrue("Is Done ? ",future.isDone());
      Assert.assertEquals("Reponse :", message, future.get());
      Assert.assertNull("No Error", future.getError());
      Assert.assertFalse("Listener Cancelled ?", listener.isCancelled());
      Assert.assertTrue("Listener Is Done ? ",listener.isDone());
      Assert.assertTrue("listener No Error :", listener.getError().isEmpty());
      Assert.assertEquals("listener Message", message, listener.getMessage().get(key1));
      executor.shutdown();
    }
  }

  /**
   * Same class used both as a listener and the one that blocks on get().
   */
  private static class ResponseCompositeFutureClientRunnerListener implements Runnable
  {
    private boolean _isDone;
    private boolean _isCancelled;
    private Map<String,String> _message;
    private Map<String,Throwable> _errorMap;
    private final ConjunctiveCompositeFuture<String, String> _future;
    private final CountDownLatch _latch = new CountDownLatch(1);
    private final CountDownLatch _endLatch = new CountDownLatch(1);

    public ResponseCompositeFutureClientRunnerListener(ConjunctiveCompositeFuture<String, String> f)
    {
      _future = f;
    }

    public void waitForAboutToGet() throws InterruptedException
    {
      _latch.await();
    }

    public void waitForDone() throws InterruptedException
    {
      _endLatch.await();
    }

    @Override
    public synchronized void run() {
      LOG.info("Running Future runner !!");

      Map<String,String> message = null;

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

      if ( null != message)
      {
        _message = message;
      }
      _errorMap = _future.getError();
      _endLatch.countDown();
      LOG.info("End Running Listener !!");
    }

    public boolean isDone() {
      return _isDone;
    }

    public boolean isCancelled() {
      return _isCancelled;
    }

    public Map<String,String> getMessage() {
      return _message;
    }

    public Map<String,Throwable> getError() {
      return _errorMap;
    }

    public ConjunctiveCompositeFuture<String, String> getFuture() {
      return _future;
    }
  }


}
