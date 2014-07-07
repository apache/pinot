package com.linkedin.pinot.transport.common;

import java.util.ArrayList;
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

public class TestSelectingFuture {
  protected static Logger LOG = LoggerFactory.getLogger(TestSelectingFuture.class);

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  @Test
  /**
   * 3 futures. Happy path. we got response from the first future
   * @throws Exception
   */
  public void testMultiFutureComposite1() throws Exception {
    List<String> keys = new ArrayList<String>();
    int numFutures = 3;
    List<KeyedFuture<String, String>> futureList =
        new ArrayList<KeyedFuture<String, String>>();
    String expectedMessage = null;
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureList.add(future);
    }
    SelectingFuture<String, String> compositeFuture = new SelectingFuture<String, String>("test");
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
      AsyncResponseFuture<String, String> future = (AsyncResponseFuture<String, String>)futureList.get(i);
      future.onSuccess(message);
    }
    expectedMessage = "dummy Message_0";
    runner.waitForDone();
    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertNull("Composite No Error :", runner.getError());
    Assert.assertEquals("Response", expectedMessage, runner.getMessage());
    Assert.assertFalse("First response not cancelled", futureList.get(0).isCancelled());
    Assert.assertTrue("Second response cancelled", futureList.get(1).isCancelled());
    Assert.assertTrue("Third response cancelled", futureList.get(2).isCancelled());
    Assert.assertNull(futureList.get(0).getError());
    Assert.assertNull(futureList.get(1).getError());
    Assert.assertNull(futureList.get(2).getError());
    Assert.assertEquals(runner.getMessage(),futureList.get(0).getOne());
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
    List<String> keys = new ArrayList<String>();
    int numFutures = 3;
    List<KeyedFuture<String, String>> futureList =
        new ArrayList<KeyedFuture<String, String>>();
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureList.add(future);
    }
    SelectingFuture<String, String> compositeFuture = new SelectingFuture<String, String>("abc");
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
      AsyncResponseFuture<String, String> future = (AsyncResponseFuture<String, String>)futureList.get(i);
      future.onError(error);
    }
    Throwable expectedError = error;
    runner.waitForDone();
    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertNull("Composite No Message :", runner.getMessage());
    Assert.assertEquals("Error", expectedError, runner.getError());
    Assert.assertFalse("First response not cancelled", futureList.get(0).isCancelled());
    Assert.assertFalse("Second response not cancelled", futureList.get(1).isCancelled());
    Assert.assertFalse("Third response not cancelled", futureList.get(2).isCancelled());
    Assert.assertNotNull(futureList.get(0).getError());
    Assert.assertNotNull(futureList.get(1).getError());
    Assert.assertEquals(runner.getError(), futureList.get(2).getError().get("key_2"));
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
    List<String> keys = new ArrayList<String>();
    int numFutures = 3;
    List<KeyedFuture<String, String>> futureList =
        new ArrayList<KeyedFuture<String, String>>();
    for (int i = 0; i < numFutures; i++) {
      String key = "key_" + i;
      keys.add(key);
      AsyncResponseFuture<String, String> future = new AsyncResponseFuture<String, String>(key);
      futureList.add(future);
    }
    SelectingFuture<String, String> compositeFuture = new SelectingFuture<String, String>("test");
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
    AsyncResponseFuture<String, String> future = (AsyncResponseFuture<String, String>)futureList.get(0);
    future.onError(error);
    //Second future gets a response
    message = "dummy Message_" + 1;
    future = (AsyncResponseFuture<String, String>)futureList.get(1);
    future.onSuccess(message);
    expectedMessage = message;

    // Third future gets a response
    message = "dummy Message_" + 2;
    error = new Exception(message);
    future = (AsyncResponseFuture<String, String>)futureList.get(2);
    future.onError(error);

    runner.waitForDone();
    Assert.assertFalse("Composite Cancelled ?", runner.isCancelled());
    Assert.assertTrue("Composite Is Done ? ", runner.isDone());
    Assert.assertNull("Composite No Error :", runner.getError());
    Assert.assertEquals("Response", expectedMessage, runner.getMessage());
    Assert.assertFalse("First response not cancelled", futureList.get(0).isCancelled());
    Assert.assertFalse("Second response not cancelled", futureList.get(1).isCancelled());
    Assert.assertTrue("Third response cancelled", futureList.get(2).isCancelled());
    Assert.assertNotNull(futureList.get(0).getError());
    Assert.assertNull(futureList.get(1).getError());
    Assert.assertNull(futureList.get(2).getError());
    Assert.assertNull(futureList.get(0).get());
    Assert.assertEquals(runner.getMessage(),futureList.get(1).getOne());
    Assert.assertNull(futureList.get(2).get());
    executor.shutdown();
  }

  /**
   * Same class used both as a listener and the one that blocks on get().
   */
  private static class ResponseCompositeFutureClientRunnerListener implements Runnable
  {
    private boolean _isDone;
    private boolean _isCancelled;
    private String _message;
    private Throwable _error;
    private final SelectingFuture<String, String> _future;
    private final CountDownLatch _latch = new CountDownLatch(1);
    private final CountDownLatch _endLatch = new CountDownLatch(1);

    public ResponseCompositeFutureClientRunnerListener(SelectingFuture<String, String> f)
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

      if ( null != message)
      {
        _message = message;
      }
      Map<String, Throwable> t = _future.getError();
      if ((null != t) && (! t.isEmpty()))
      {
        _error = t.values().iterator().next();
      }
      _endLatch.countDown();
      LOG.info("End Running Listener !!");
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

    public SelectingFuture<String, String> getFuture() {
      return _future;
    }
  }
}
