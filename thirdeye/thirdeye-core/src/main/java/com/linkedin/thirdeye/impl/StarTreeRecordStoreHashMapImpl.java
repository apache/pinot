package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StarTreeRecordStoreHashMapImpl implements StarTreeRecordStore {
  private final StarTreeConfig config;
  private final ConcurrentMap<DimensionKey, MetricTimeSeries> store;
  private final AtomicLong minTime;
  private final AtomicLong maxTime;
  private final MetricSchema metricSchema;
  private final ReadWriteLock lock;

  public StarTreeRecordStoreHashMapImpl(StarTreeConfig config) {
    this.config = config;
    this.store = new ConcurrentHashMap<>();
    this.minTime = new AtomicLong(-1);
    this.maxTime = new AtomicLong(-1);
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    this.lock = new ReentrantReadWriteLock(true);
  }

  @Override
  public void update(StarTreeRecord record) {
    lock.writeLock().lock();
    try {
      MetricTimeSeries existing = store.putIfAbsent(record.getDimensionKey(), record.getMetricTimeSeries());
      if (existing != null) {
        existing.aggregate(record.getMetricTimeSeries());
      }
      setMinTime(Collections.min(record.getMetricTimeSeries().getTimeWindowSet()));
      setMaxTime(Collections.max(record.getMetricTimeSeries().getTimeWindowSet()));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Iterator<StarTreeRecord> iterator() {
    lock.readLock().lock();
    try {
      List<StarTreeRecord> records = new ArrayList<>(store.size());
      for (Map.Entry<DimensionKey, MetricTimeSeries> entry : store.entrySet()) {
        records.add(new StarTreeRecordImpl(config, entry.getKey(), entry.getValue()));
      }
      return records.iterator();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      store.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void open() throws IOException {
    // NOP
  }

  @Override
  public void close() throws IOException {
    // NOP
  }

  @Override
  public int getRecordCount() {
    lock.readLock().lock();
    try {
      return store.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getRecordCountEstimate() {
    lock.readLock().lock();
    try {
      return store.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public int getCardinality(String dimensionName) {
    return getDimensionValues(dimensionName).size();
  }

  @Override
  public String getMaxCardinalityDimension() {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist) {
    lock.readLock().lock();
    try {
      String maxName = null;
      int max = 0;

      for (DimensionSpec dimensionSpec : config.getDimensions()) {
        Set<String> values = getDimensionValues(dimensionSpec.getName());
        if (values.size() >= max) {
          max = values.size();
          maxName = dimensionSpec.getName();
        }
      }

      return maxName;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName) {
    lock.readLock().lock();
    try {
      int dimensionIndex = -1;
      for (int i = 0; i < config.getDimensions().size(); i++) {
        if (config.getDimensions().get(i).getName().equals(dimensionName)) {
          dimensionIndex = i;
          break;
        }
      }

      if (dimensionIndex == -1) {
        throw new IllegalArgumentException("No such dimension " + dimensionName);
      }

      Set<String> values = new HashSet<>();

      for (DimensionKey key : store.keySet()) {
        values.add(key.getDimensionValues()[dimensionIndex]);
      }

      return values;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Number[] getMetricSums(StarTreeQuery query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Long getMinTime() {
    return minTime.get();
  }

  @Override
  public Long getMaxTime() {
    return maxTime.get();
  }

  @Override
  public Map<TimeRange, Integer> getTimeRangeCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query) {
    lock.readLock().lock();
    try {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

      for (Map.Entry<DimensionKey, MetricTimeSeries> entry : store.entrySet()) {
        boolean matches = true;

        for (int i = 0; i < config.getDimensions().size(); i++) {
          String queryValue = query.getDimensionKey().getDimensionValues()[i];
          String recordValue = entry.getKey().getDimensionValues()[i];
          if (!StarTreeConstants.STAR.equals(queryValue) && !queryValue.equals(recordValue)) {
            matches = false;
            break;
          }
        }

        if (matches) {
          timeSeries.aggregate(entry.getValue(), query.getTimeRange());
        }
      }

      return timeSeries;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Map<String, Map<String, Integer>> getForwardIndex() {
    lock.readLock().lock();
    try {
      Map<String, Map<String, Integer>> forwardIndex = new HashMap<>();

      for (DimensionSpec dimensionSpec : config.getDimensions()) {
        Map<String, Integer> dimensionIndex = new HashMap<>();
        dimensionIndex.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
        dimensionIndex.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
        forwardIndex.put(dimensionSpec.getName(), dimensionIndex);
      }

      int currentId = StarTreeConstants.FIRST_VALUE;

      for (DimensionKey key : store.keySet()) {
        for (int i = 0; i < config.getDimensions().size(); i++) {
          String name = config.getDimensions().get(i).getName();
          String value = key.getDimensionValues()[i];
          Integer id = forwardIndex.get(name).get(value);
          if (id == null) {
            forwardIndex.get(name).put(value, currentId);
            currentId++;
          }
        }
      }

      return forwardIndex;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void setMinTime(long localMinTime) {
    while (true) {
      long existingMin = minTime.get();
      if (existingMin == -1 || localMinTime < existingMin) {
        if (!minTime.compareAndSet(existingMin, localMinTime)) {
          continue;
        }
      }
      break;
    }
  }

  private void setMaxTime(long localMaxTime) {
    while (true) {
      long existingMax = maxTime.get();
      if (existingMax == -1 || localMaxTime > existingMax) {
        if (!maxTime.compareAndSet(existingMax, localMaxTime)) {
          continue;
        }
      }
      break;
    }
  }
}
