package com.linkedin.pinot.transport.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.transport.common.ServerInstance;

/**
 * A Netty standalone connection. This will be managed as a resource in a pool to reuse
 * connection. This is not thread-safe so multiple requests cannot be sent simultaneously.
 * This class provides an async API to send requests and wait for response.
 */
public abstract class NettyClientConnection
{
  protected static Logger LOG = LoggerFactory.getLogger(NettyTCPClientConnection.class);

  /**
   * Client Connection State
   */
  public enum State
  {
    INIT,
    CONNECTED,
    REQUEST_WRITTEN,
    REQUEST_SENT,
    ERROR,
    GOT_RESPONSE;

    public boolean isValidTransition(State nextState)
    {
      switch (nextState)
      {
        case INIT: return false; // Init state happens only as the first transition
        case CONNECTED: return this == State.INIT;  // We do not reconnect with same NettyClientConnection object. We create new one
        case REQUEST_WRITTEN: return this == State.CONNECTED || this == State.GOT_RESPONSE;
        case REQUEST_SENT: return this == State.REQUEST_WRITTEN;
        case ERROR: return true;
        case GOT_RESPONSE: return this == State.REQUEST_SENT;
      }
      return false;
    }
  };

  protected final ServerInstance _server;

  protected final EventLoopGroup _eventGroup;
  protected Bootstrap _bootstrap;
  protected Channel _channel;
  // State of the request/connection
  protected State _connState;

  public NettyClientConnection(ServerInstance server, EventLoopGroup eventGroup)
  {
    _connState = State.INIT;
    _server = server;
    _eventGroup = eventGroup;
  }

  /**
   * Connect to the server. Returns false if unable to connect to the server.
   */
  public abstract boolean connect();

  /**
   * Close the client connection
   */
  public abstract void close() throws InterruptedException;

  /**
   * API to send a request asynchronously.
   * @param serializedRequest serialized payload to send the request
   * @return Future to return the response returned from the server.
   */
  public abstract ResponseFuture sendRequest(ByteBuf serializedRequest);


  /**
   * Future Handle provided to the request sender to asynchronously wait for response.
   * We use guava API for implementing Futures.
   */
  public static class ResponseFuture implements ListenableFuture<ByteBuf>
  {
    // Lock for mutex
    private final Lock _futureLock = new ReentrantLock();
    // Condition variable to wait for the response
    private final Condition _finished = _futureLock.newCondition();
    // Flag to track completion
    private final AtomicBoolean _isDone = new AtomicBoolean(false);
    /**
     *  Serialized response.
     *  If the future is cancelled or in case of error, this will be null.
     *  In that case, clients will need to use specific APIs to distinguish
     *  between cancel and errors.
     */
    private volatile ByteBuf _serializedResponse;
    // Exception in case of error
    private volatile Throwable _error;
    // List of runnables that needs to be executed on completion
    private final List<Runnable> _pendingRunnable = new ArrayList<Runnable>();
    //List of executors that needs to run the runnables.
    private final List<Executor> _pendingRunnableExecutors = new ArrayList<Executor>();

    /**
     * Response Future State
     */
    private enum State
    {
      PENDING,
      CANCELLED,
      SUCCESS,
      FAILED;

      public boolean isCompleted()
      {
        return this != PENDING;
      }
    }

    // State of the future
    private State _state;

    public ResponseFuture()
    {
      _state = State.PENDING;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      try
      {
        _futureLock.lock();
        if ( _state.isCompleted())
        {
          LOG.info("Request is no longer pending. Cannot cancel !!");
          return false ;
        }
        setDone(State.CANCELLED);
      } finally {
        _futureLock.unlock();
      }
      return true;
    }

    public void processResponse(ByteBuf result)
    {
      try
      {
        _futureLock.lock();
        if ( _state == State.CANCELLED)
        {
          LOG.info("Request has already been cancelled. Discarding response !!");
          return;
        }
        _serializedResponse = result;
        setDone(State.SUCCESS);
      } finally {
        _futureLock.unlock();
      }
    }

    /**
     * Set Exception and let the future listener get notified.
     * @param t throwable
     */
    public void processError(Throwable t)
    {
      try
      {
        _futureLock.lock();
        if ( _state == State.CANCELLED)
        {
          LOG.info("Request has already been cancelled. Discarding response !!");
          return;
        }
        _error = t;
        setDone(State.FAILED);
      } finally {
        _futureLock.unlock();
      }
    }

    @Override
    public boolean isCancelled()
    {
      return _state == State.CANCELLED;
    }

    @Override
    public boolean isDone()
    {
      return _isDone.get();
    }

    @Override
    public ByteBuf get() throws InterruptedException,
    ExecutionException
    {
      try
      {
        _futureLock.lock();
        while( ! _isDone.get() )
        {
          _finished.await();
        }
      } finally {
        _futureLock.unlock();
      }
      return _serializedResponse;
    }

    public Throwable getError()
    {
      return _error;
    }

    @Override
    public ByteBuf get(long timeout, TimeUnit unit) throws InterruptedException,
    ExecutionException, TimeoutException
    {
      try
      {
        _futureLock.lock();
        while( ! _isDone.get() )
        {
          boolean notElapsed = _finished.await(timeout, unit);
          if (!notElapsed)
          {
            throw new TimeoutException("Timeout awaiting response !!");
          }
        }
      } finally {
        _futureLock.unlock();
      }
      return _serializedResponse;
    }


    /**
     * Mark complete and notify threads waiting for this condition
     */
    private void setDone(State state)
    {
      LOG.info("Setting state to :" + state + ", Current State :" + _state);
      try
      {
        _futureLock.lock();
        _isDone.set(true);
        _state = state;
        _finished.signalAll();
      } finally {
        _futureLock.unlock();
      }
      for (int i=0; i < _pendingRunnable.size();i++)
      {
        LOG.info("Running pending runnable :" + i);
        _pendingRunnableExecutors.get(i).execute(_pendingRunnable.get(i));
      }
      _pendingRunnable.clear();
      _pendingRunnableExecutors.clear();
    }

    @Override
    public void addListener(Runnable listener, Executor executor)
    {
      boolean processed = false;
      try
      {
        _futureLock.lock();
        if ( ! _state.isCompleted() )
        {
          _pendingRunnable.add(listener);
          _pendingRunnableExecutors.add(executor);
          processed = true;
        }
      } finally {
        _futureLock.unlock();
      }

      if ( ! processed )
      {
        LOG.info("Executing the listener as the future event is already done !!");
        executor.execute(listener);
      }
    }
  }
}

