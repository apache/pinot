package org.apache.pinot.query.runtime.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.utils.JsonUtils;


public class StageStats {

  // use memoized supplier so that the timing doesn't start until the
  // first time we get the timer
  private final Supplier<ThreadResourceUsageProvider> _exTimer =
      Suppliers.memoize(ThreadResourceUsageProvider::new)::get;

  // this is used to make sure that toString() doesn't have side
  // effects (accidentally starting the timer)
  private volatile boolean _exTimerStarted = false;

  private final Stopwatch _executeStopwatch = Stopwatch.createUnstarted();
  private final Stopwatch _queuedStopwatch = Stopwatch.createUnstarted();
  private final AtomicLong _queuedCount = new AtomicLong();
  private final StatMap<StatKey> _ownStats = new StatMap<>(StatKey.class);

  private final MultiStageOperator<?> _root;
  private final ConcurrentHashMap<MultiStageOperator<?>, StatMap<?>> _operatorStatsMap = new ConcurrentHashMap<>();

  public StageStats(MultiStageOperator<?> root) {
    _root = root;
  }

  public void executing() {
    startExecutionTimer();
    if (_queuedStopwatch.isRunning()) {
      long elapsed = _queuedStopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
      _queuedStopwatch.reset();
      _ownStats.add(StatKey.QUEUED_TIME, elapsed);
    }
  }

  private void startExecutionTimer() {
    _exTimerStarted = true;
    _exTimer.get();
    if (!_executeStopwatch.isRunning()) {
      _executeStopwatch.start();
    }
  }

  public void queued() {
    _queuedCount.incrementAndGet();
    _ownStats.add(StatKey.QUEUED_COUNT, 1);
    if (!_queuedStopwatch.isRunning()) {
      _queuedStopwatch.start();
    }
    if (_executeStopwatch.isRunning()) {
      long elapsed = _executeStopwatch.stop().elapsed(TimeUnit.MILLISECONDS);
      _executeStopwatch.reset();
      _ownStats.add(StatKey.EXECUTION_TIME, elapsed);
    }
  }

  public long getExecutionTime() {
    return _executeStopwatch.elapsed(TimeUnit.MILLISECONDS);
  }

  public JsonNode asJson() {
    ObjectNode json = _ownStats.asJson();

    ArrayNode operationStats = JsonUtils.newArrayNode();
    _root.forEachOperator(op -> {
      StatMap<?> stats = _operatorStatsMap.get(op);
      if (stats == null) {
        operationStats.addNull();
      } else {
        operationStats.add(stats.asJson());
      }
    });
    json.set("operationStats", operationStats);

    return json;
  }

  public void serialize(DataOutput output)
      throws IOException {
    _ownStats.serialize(output);
    try {
      _root.forEachOperator(op -> {
        try {
          StatMap<?> stats = _operatorStatsMap.get(op);
          if (stats == null) {
            output.writeBoolean(false);
          } else {
            output.writeBoolean(true);
            stats.serialize(output);
          }
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
      });
    } catch (UncheckedIOException ex) {
      throw ex.getCause();
    }
  }
  /**
   * Merges the stats from another StageStats object into this one.
   *
   * This object is modified in place while the other object is not modified.
   * Both stats must belong to the same stage.
   */
  // We need to deal with unchecked because Java type system is not expressive enough to handle this at static time
  // But we know we are merging the same stat types because they were created for the same operation.
  // There is also a dynamic check in the StatMap.merge() method.
  @SuppressWarnings("unchecked")
  public void merge(StageStats other) {
    _ownStats.merge(other._ownStats);
    _root.forEachOperator(op -> {
      StatMap otherStats = other._operatorStatsMap.get(op);
      StatMap myStats = _operatorStatsMap.get(op);
      if (myStats == null) {
        _operatorStatsMap.put(op, otherStats);
      } else if (otherStats != null) {
        myStats.merge(otherStats);
      }
    });
  }

  public void merge(DataInput input)
      throws IOException {
    _ownStats.merge(input);
    try {
      _root.forEachOperator(op -> {
        try {
          if (input.readBoolean()) {
            _operatorStatsMap.compute(op, (opKey, existingStats) -> {
              try {
                StatMap<?> result = existingStats == null ? new StatMap<>(op.getStatKeyClass()) : existingStats;
                result.merge(input);
                return result;
              } catch (IOException ex) {
                throw new UncheckedIOException(ex);
              }
            });
          }
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
      });
    } catch (UncheckedIOException ex) {
      throw ex.getCause();
    }
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME(StatMap.Type.LONG),
    QUEUED_TIME(StatMap.Type.LONG),
    QUEUED_COUNT(StatMap.Type.INT);
    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
