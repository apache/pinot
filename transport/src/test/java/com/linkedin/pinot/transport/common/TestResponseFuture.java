package com.linkedin.pinot.transport.common;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.ImmediateExecutor;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.Checkable;
import com.linkedin.pinot.TestUtils;

public class TestResponseFuture {
  protected static Logger LOG = LoggerFactory.getLogger(TestResponseFuture.class);

  /*
   * Future Handle provided to the request sender to asynchronously wait for response.
   * We use guava API for implementing Futures.
   */
  public static class ResponseFuture extends AsyncResponseFuture<ServerInstance, ByteBuf>
  {

    public ResponseFuture(ServerInstance key) {
      super(key);
    }

  }

  static
  {
    org.apache.log4j.Logger.getRootLogger().addAppender(new ConsoleAppender(
        new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN), "System.out"));
  }

  @Test
  public void testResponseFutureOtherCases() throws Exception
  {
    ServerInstance s = new ServerInstance("localhost", 8080);

    // A Future is cancelled but gets a message after that. get() called before cancellation
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      f.cancel(false);
      String message = "dummy Message";
      setResponse(f, message);
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // A Future is cancelled but gets a message after that. get() called after cancellation
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      f.cancel(false);
      String message = "dummy Message";
      setResponse(f, message);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // A Future is cancelled but gets an exception after that. get() called before cancellation
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      f.cancel(false);
      f.onError(new Exception("dummy"));
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // A Future is cancelled but gets an exception after that. get() called after cancellation
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      f.cancel(false);
      f.onError(new Exception("dummy"));
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Then we try to cancel. Future Client calls get() before exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      f.cancel(false);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Then we try to cancel. Future Client calls get() after exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      f.cancel(false);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }

    // Set Response. Then we try to cancel. Future Client calls get() before response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      String message = "dummy Message";
      setResponse(f, message);
      f.cancel(false);
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Set Response. Then we try to cancel. Future Client calls get() after response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      String message = "dummy Message";
      setResponse(f, message);
      f.cancel(false);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Set Response. Then an exception happens. Future Client calls get() before response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      String message = "dummy Message";
      setResponse(f, message);
      f.onError(new Exception("dummy"));
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Set Response. then an exception gets processed. Future Client calls get() after response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      String message = "dummy Message";
      setResponse(f, message);
      f.onError(new Exception("dummy"));
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Then a valid response arrives. Future Client calls get() before exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      String message = "dummy Message";
      setResponse(f, message);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Then a valid response arrives. Future Client calls get() after exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      String message = "dummy Message";
      setResponse(f, message);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }
  }

  @Test
  public void testResponseFuture() throws Exception
  {
    ServerInstance s = new ServerInstance("localhost", 8080);
    //Cancelled Future. Future Client calls get() before cancel()
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      f.cancel(false);
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    //Cancelled Future. Future Client calls get() after cancel()
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      f.cancel(false);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForDone();
      Assert.assertTrue("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() before exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }

    // Throw Exception. Future Client calls get() after exception
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForDone();
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertNull("No Reponse :", runner.getMessage());
      Assert.assertEquals("Error", expectedError, runner.getError());
      executor.shutdown();
    }

    // Set Response. Future Client calls get() before response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForAboutToGet(); // No guarantees as this only ensures the thread is started but not blocking in get().
      Thread.sleep(100);
      String message = "dummy Message";
      setResponse(f, message);
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }

    // Set Response. Future Client calls get() after response
    {
      ResponseFuture f = new ResponseFuture(s);
      ResponseFutureClientRunner runner = new ResponseFutureClientRunner(f);
      String message = "dummy Message";
      setResponse(f, message);
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
      executor.execute(runner);
      runner.waitForDone();
      // Now we know runner executed
      Assert.assertFalse("Cancelled ?", runner.isCancelled());
      Assert.assertTrue("Is Done ? ",runner.isDone());
      Assert.assertEquals("Response Check:", message, runner.getMessage());
      Assert.assertNull("No Error :", runner.getError());
      executor.shutdown();
    }
  }

  @Test
  public void testResponseFutureListener() throws Exception
  {
    ServerInstance s = new ServerInstance("localhost", 8080);

    // Cancelled future. Listener added before cancelling
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      WaitForCompletion w = new WaitForCompletion(listener);
      f.cancel(false);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertTrue("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertNull("No Reponse :", listener.getMessage());
      Assert.assertNull("No Error :", listener.getError());
      listener.close();
    }

    // Cancelled future. Listener added after cancelling
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      WaitForCompletion w = new WaitForCompletion(listener);
      f.cancel(false);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertTrue("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertNull("No Reponse :", listener.getMessage());
      Assert.assertNull("No Error :", listener.getError());
      listener.close();
    }

    // Throw Exception. Listener added before exception
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      WaitForCompletion w = new WaitForCompletion(listener);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertNull("No Reponse :", listener.getMessage());
      Assert.assertEquals("Error", expectedError, listener.getError());
    }

    // Throw Exception. Listener added after exception
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      Exception expectedError = new Exception("error processing");
      f.onError(expectedError);
      WaitForCompletion w = new WaitForCompletion(listener);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertNull("No Reponse :", listener.getMessage());
      Assert.assertEquals("Error", expectedError, listener.getError());
    }

    // Set Response. Listener added before response
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      WaitForCompletion w = new WaitForCompletion(listener);
      String message = "dummy Message";
      setResponse(f, message);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertEquals("Response Check:", message, listener.getMessage());
      Assert.assertNull("No Error :", listener.getError());
      listener.close();
    }

    // Set Response. Listener added after response
    {
      ResponseFuture f = new ResponseFuture(s);
      FutureListener listener = new FutureListener(f);
      String message = "dummy Message";
      setResponse(f, message);
      WaitForCompletion w = new WaitForCompletion(listener);
      TestUtils.assertWithBackoff(w, 10, 1000, 2, 1, 2);
      // Now we know listener executed
      Assert.assertFalse("Cancelled ?", listener.isCancelled());
      Assert.assertEquals("Num Runs of listener", 1, listener.getNumRuns());
      Assert.assertTrue("Is Done ? ",listener.isDone());
      Assert.assertEquals("Response Check:", message, listener.getMessage());
      Assert.assertNull("No Error :", listener.getError());
      listener.close();
    }
  }

  private static void setResponse(ResponseFuture f, String message)
  {
    ByteBuf b = Unpooled.wrappedBuffer(message.getBytes());
    f.onSuccess(b);
  }

  private static class WaitForCompletion implements Checkable
  {
    private final FutureListener _listener;
    public WaitForCompletion(FutureListener f)
    {
      _listener = f;
    }

    @Override
    public boolean runCheck() throws AssertionError {
      boolean isCalled = false;
      synchronized(_listener)
      {
        isCalled = _listener.isCalled();
      }
      return isCalled;
    }
  }

  private static class ResponseFutureClientRunner implements Runnable
  {
    private boolean _isDone;
    private boolean _isCancelled;
    private String _message;
    private Throwable _error;
    private final ResponseFuture _future;
    private final CountDownLatch _latch = new CountDownLatch(1);
    private final CountDownLatch _endLatch = new CountDownLatch(1);

    public ResponseFutureClientRunner(ResponseFuture f)
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

      ByteBuf message = null;

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
        byte[] m = new byte[message.readableBytes()];
        message.readBytes(m);
        _message = new String(m);
      }

      Map<ServerInstance, Throwable> errorMap = _future.getError();
      if ((null!=errorMap) && (!errorMap.isEmpty()))
      {
        _error = errorMap.values().iterator().next();
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

    public ResponseFuture getFuture() {
      return _future;
    }
  }

  private static class FutureListener
  implements Runnable
  {
    private int _numRuns = 0;
    private boolean _isCalled;
    private boolean _isDone;
    private boolean _isCancelled;
    private String _message;
    private Throwable _error;
    private final ResponseFuture _future;
    private Executor _executor = null;

    public FutureListener(ResponseFuture f)
    {
      _future = f;
      LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
      queue.add(this);
      //_executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.HOURS, queue);
      _executor = ImmediateExecutor.INSTANCE;
      f.addListener(this, _executor);
    }

    @Override
    public synchronized void run() {
      _isCalled = true;
      _numRuns++;
      LOG.info("Running Listener !!");
      //try{ throw new Exception("dummy"); } catch(Exception e) {e.printStackTrace();}
      _isDone = _future.isDone();
      _isCancelled = _future.isCancelled();

      ByteBuf message = null;

      try {
        message = _future.getOne();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (ExecutionException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      if ( null != message)
      {
        byte[] m = new byte[message.readableBytes()];
        message.readBytes(m);
        _message = new String(m);
      }
      Map<ServerInstance, Throwable> errorMap = _future.getError();

      if ((null != errorMap) && (!errorMap.isEmpty())) {
        _error = errorMap.values().iterator().next();
      }

      LOG.info("End Running Listener !!");
    }

    public void close()
    {
      //_executor.shutdown();
    }

    public boolean isCalled() {
      return _isCalled;
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

    public int getNumRuns() {
      return _numRuns;
    }
  }
}
