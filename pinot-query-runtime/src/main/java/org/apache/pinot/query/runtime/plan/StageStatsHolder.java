package org.apache.pinot.query.runtime.plan;

import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.StageStats;

/**
 * A stats holder for stages.
 */
public class StageStatsHolder {
  private final int _currentStageId;
  /**
   * The list of stats.
   *
   * This list may grow as the stats are merged. This is because leaf stages send an array of stats as long as their
   * own stage id because they do not know how many extra stage may be in other parts of the query plan.
   */
  private final List<StageStats> _allStats;

  private StageStatsHolder(int stageId, StageStats currentStageStats, List<ByteBuffer> allUpstreamStats) {
    _currentStageId = stageId;
    _allStats = new ArrayList<>(allUpstreamStats.size());
    for (int i = 0; i < allUpstreamStats.size(); i++) {
      ByteBuffer originalBuf = allUpstreamStats.get(i);
      if (i < stageId) {
        Preconditions.checkArgument(originalBuf == null, "Stats for stage %s is not null", i);
        _allStats.add(null);
      } else if (i == stageId) {
        Preconditions.checkArgument(originalBuf == null, "Stats for stage %s is not null", i);
        _allStats.add(currentStageStats);
      } else {
        if (originalBuf == null) {
          _allStats.add(null);
          continue;
        }
        ByteBuffer byteBuffer = originalBuf.duplicate();
        byteBuffer.order(originalBuf.order());
        byteBuffer.clear();
        try (InputStream is = new ByteBufferInputStream(Collections.singletonList(byteBuffer));
            DataInputStream dis = new DataInputStream(is)) {
          _allStats.add(StageStats.deserialize(dis));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  public static StageStatsHolder create(int stageId, StageStats currentStageStats) {
    List<ByteBuffer> allUpstreamStats = new ArrayList<>(stageId);
    for (int i = 0; i < stageId; i++) {
      allUpstreamStats.add(null);
    }
    return new StageStatsHolder(stageId, currentStageStats, allUpstreamStats);
  }

  public static StageStatsHolder create(int stageId, MultiStageOperator.Type type, @Nullable StatMap<?> opStats) {
    StageStats stageStats = new StageStats();
    stageStats.addLastOperator(type, opStats);
    return StageStatsHolder.create(stageId, stageStats);
  }

  public List<ByteBuffer> serialize()
      throws IOException {
    List<ByteBuffer> serializedStats = new ArrayList<>(_allStats.size());
    for (StageStats stageStats : _allStats) {
      if (stageStats == null) {
        serializedStats.add(null);
      } else {
        try (UnsynchronizedByteArrayOutputStream baos = new UnsynchronizedByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(baos)) {
          stageStats.serialize(output);
          ByteBuffer buf = ByteBuffer.wrap(baos.toByteArray());
          serializedStats.add(buf);
        }
      }
    }
    return serializedStats;
  }

  public StageStats getCurrentStats() {
    return _allStats.get(_currentStageId);
  }

  public StageStats getStageStats(int stageId) {
    Preconditions.checkArgument(stageId >= _currentStageId, "Stage %s is not available at stage %s",
        stageId, _currentStageId);
    return _allStats.get(stageId);
  }

  public void merge(StageStatsHolder otherStats) {
    int maxCommonStages = Math.min(_allStats.size(), otherStats._allStats.size());
    for (int i = 0; i < maxCommonStages; i++) {
      StageStats myStats = _allStats.get(i);
      StageStats otherStatsForStage = otherStats._allStats.get(i);
      if (myStats == null) {
        if (otherStatsForStage != null) {
          _allStats.set(i, otherStatsForStage);
        }
      } else if (otherStatsForStage != null) {
        myStats.merge(otherStatsForStage);
      }
    }
    for (int i = maxCommonStages; i < otherStats._allStats.size(); i++) {
      _allStats.add(otherStats._allStats.get(i));
    }
  }

  public void merge(List<ByteBuffer> otherStats) throws IOException {
    int min = Math.min(_allStats.size(), otherStats.size());
    for (int i = 0; i < min; i++) {
      StageStats myStats = _allStats.get(i);
      ByteBuffer otherBuf = otherStats.get(i);
      if (myStats == null) {
        if (otherBuf != null) {
          try (InputStream is = new ByteBufferInputStream(Collections.singletonList(otherBuf));
              DataInputStream dis = new DataInputStream(is)) {
            _allStats.set(i, StageStats.deserialize(dis));
          }
        }
      } else if (otherBuf != null) {
        try (InputStream is = new ByteBufferInputStream(Collections.singletonList(otherBuf));
            DataInputStream dis = new DataInputStream(is)) {
          myStats.merge(StageStats.deserialize(dis));
        }
      }
    }
    for (int i = min; i < otherStats.size(); i++) {
      ByteBuffer otherBuf = otherStats.get(i);
      if (otherBuf != null) {
        try (InputStream is = new ByteBufferInputStream(Collections.singletonList(otherBuf));
            DataInputStream dis = new DataInputStream(is)) {
          _allStats.add(StageStats.deserialize(dis));
        }
      } else {
        _allStats.add(null);
      }
    }
  }
}
