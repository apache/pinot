package org.apache.pinot.spi.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.MDC;

/**
 * The {@code QueryThreadContext} class is a thread-local context for storing common query-related information
 * associated to the current thread.
 *
 * <p>It is used to pass information between different layers of the query execution stack without changing the
 * method signatures. This is also used to populate the {@link MDC} context for logging.
 */
public abstract class QueryThreadContext implements AutoCloseable {
  private static final ThreadLocal<QueryThreadContext> THREAD_LOCAL = new ThreadLocal<>();

  private QueryThreadContext() {
  }

  public static QueryThreadContext get() {
    return Preconditions.checkNotNull(THREAD_LOCAL.get(), "QueryThreadContext is not initialized");
  }

  public static boolean isInitialized() {
    return THREAD_LOCAL.get() != null;
  }

  public static QueryThreadContext open() {
    return open(null);
  }

  public static QueryThreadContext open(@Nullable Memento memento) {
    Preconditions.checkState(THREAD_LOCAL.get() == null, "QueryThreadContext is already initialized");

    QueryThreadContext context = new BaseLayer();
    if (memento != null) {
      context.setStartTimeMsProtected(memento._startTimeMs);
      context.setDeadlineMsProtected(memento._deadlineMs);
      context.setTimeoutMsProtected(memento._timeoutMs);
      context.setBrokerIdProtected(memento._brokerId);
      context.setRequestIdProtected(memento._requestId);
      context.setCidProtected(memento._cid);
      context.setSqlProtected(memento._sql);
      context.setQueryTypeProtected(memento._queryType);
    }

    THREAD_LOCAL.set(context);
    return context;
  }

  public abstract long getStartTimeMs();

  protected abstract void setStartTimeMsProtected(long startTimeMs);

  public void setStartTimeMs(long startTimeMs) {
    Preconditions.checkState(getStartTimeMs() == 0, "Start time already set to %s, cannot set again",
        getStartTimeMs());
    setStartTimeMsProtected(startTimeMs);
  }

  public abstract long getDeadlineMs();

  protected abstract void setDeadlineMsProtected(long deadlineMs);

  public void setDeadlineMs(long deadlineMs) {
    Preconditions.checkState(getDeadlineMs() == 0, "Deadline already set to %s, cannot set again",
        getDeadlineMs());
    setDeadlineMsProtected(deadlineMs);
  }

  public abstract long getTimeoutMs();

  protected abstract void setTimeoutMsProtected(long timeoutMs);

  public void setTimeoutMs(long timeoutMs) {
    Preconditions.checkState(getTimeoutMs() == 0, "Timeout already set to %s, cannot set again",
        getTimeoutMs());
    setTimeoutMsProtected(timeoutMs);
  }

  public abstract String getBrokerId();

  protected abstract void setBrokerIdProtected(String brokerId);

  public void setBrokerId(String brokerId) {
    Preconditions.checkState(getBrokerId() == null, "Broker id already set to %s, cannot set again",
        getBrokerId());
    setBrokerIdProtected(brokerId);
  }

  public abstract int getRequestId();

  protected abstract void setRequestIdProtected(int requestId);

  public void setRequestId(int requestId) {
    Preconditions.checkState(getRequestId() == 0, "Request id already set to %s, cannot set again", getRequestId());
    LoggerConstants.QUERY_ID_KEY.registerInMdc(Integer.toString(requestId));
    setRequestIdProtected(requestId);
  }

  public abstract String getCid();

  protected abstract void setCidProtected(String cid);

  public void setCid(String cid) {
    Preconditions.checkState(getCid() == null, "Correlation id already set to %s, cannot set again",
        getCid());
    LoggerConstants.CORRELATION_ID_KEY.registerInMdc(cid);
    setCidProtected(cid);
  }

  public abstract String getSql();

  protected abstract void setSqlProtected(String sql);

  public void setSql(String sql) {
    Preconditions.checkState(getSql() == null, "SQL already set to %s, cannot set again",
        getSql());
    setSqlProtected(sql);
  }

  public abstract String getQueryType();

  protected abstract void setQueryTypeProtected(String queryType);

  public void setQueryType(String queryType) {
    Preconditions.checkState(getQueryType() == null, "Query type already set to %s, cannot set again",
        getQueryType());
    setQueryTypeProtected(queryType);
  }

  @Override
  public String toString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      return "Failed to convert QueryThreadContext to JSON: " + e.getMessage();
    }
  }

  public static Memento createMemento() {
    return new Memento(get());
  }

  private static class BaseLayer extends QueryThreadContext {
    private long _startTimeMs;
    private long _deadlineMs;
    private long _timeoutMs;
    private String _brokerId;
    private int _requestId;
    private String _cid;
    private String _sql;
    private String _queryType;

    @Override
    public long getStartTimeMs() {
      return _startTimeMs;
    }

    @Override
    protected void setStartTimeMsProtected(long startTimeMs) {
      _startTimeMs = startTimeMs;
    }

    @Override
    public long getDeadlineMs() {
      return _deadlineMs;
    }

    @Override
    protected void setDeadlineMsProtected(long deadlineMs) {
      _deadlineMs = deadlineMs;
    }

    @Override
    public long getTimeoutMs() {
      return _timeoutMs;
    }

    @Override
    protected void setTimeoutMsProtected(long timeoutMs) {
      _timeoutMs = timeoutMs;
    }

    @Override
    public String getBrokerId() {
      return _brokerId;
    }

    @Override
    protected void setBrokerIdProtected(String brokerId) {
      _brokerId = brokerId;
    }

    @Override
    public int getRequestId() {
      return _requestId;
    }

    @Override
    protected void setRequestIdProtected(int requestId) {
      _requestId = requestId;
    }

    @Override
    public String getCid() {
      return _cid;
    }

    @Override
    protected void setCidProtected(String cid) {
      _cid = cid;
    }

    @Override
    public String getSql() {
      return _sql;
    }

    @Override
    protected void setSqlProtected(String sql) {
      _sql = sql;
    }

    @Override
    public String getQueryType() {
      return _queryType;
    }

    @Override
    protected void setQueryTypeProtected(String queryType) {
      _queryType = queryType;
    }

    @Override
    public void close() {
      THREAD_LOCAL.remove();
      if (_requestId != 0) {
        LoggerConstants.QUERY_ID_KEY.unregisterFromMdc();
      }
      if (_cid != null) {
        LoggerConstants.CORRELATION_ID_KEY.unregisterFromMdc();
      }
    }
  }


  /**
   * The {@code Memento} class is used to capture the state of the {@link QueryThreadContext} for saving and restoring
   * the state.
   *
   * Although the <a href="https://en.wikipedia.org/wiki/Memento_pattern">Memento Design Pattern</a> is usually used to
   * keep state private, here we are using it to be sure {@link QueryThreadContext} can only be copied from threads in
   * a safe way.
   *
   * Given the only way to create a {@link QueryThreadContext} with known state is through the {@link Memento} class,
   * we can be sure that the state is always copied between threads. The alternative would be to create an
   * {@link #open()} method that accepts a {@link QueryThreadContext} as an argument, but that would allow the receiver
   * thread to use the received {@link QueryThreadContext} directly, which is not safe because it belongs to another
   * thread.
   */
  public static class Memento {
    private final long _startTimeMs;
    private final long _deadlineMs;
    private final long _timeoutMs;
    private final String _brokerId;
    private final int _requestId;
    private final String _cid;
    private final String _sql;
    private final String _queryType;

    private Memento(QueryThreadContext context) {
      _startTimeMs = context.getStartTimeMs();
      _deadlineMs = context.getDeadlineMs();
      _timeoutMs = context.getTimeoutMs();
      _brokerId = context.getBrokerId();
      _requestId = context.getRequestId();
      _cid = context.getCid();
      _sql = context.getSql();
      _queryType = context.getQueryType();
    }
  }
}
