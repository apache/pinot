package org.apache.pinot.perf;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)
public class BenchmarkHashMap {
  private static final int NUM_VALUES = 30605865;

  @Param({"1", "2", "4", "8", "20"})
  private int _numThreads;
  private static final Random RANDOM = new Random();

  private final Key[] _keys = new Key[NUM_VALUES];

  private final Key[] _copyKeys = new Key[NUM_VALUES];

  private double _rangeMin = 100;
  private double _rangeMax = 600;

  private SumAggregationFunction _sumAgg = new SumAggregationFunction(null, false);

  private final Record[] _records = new Record[NUM_VALUES];

  private ExecutorService _executorService;

  public int _uuidCardinality = 4752537;

  public String[] generateUUIDSet() {
    String[] uuidSet = new String[_uuidCardinality];
    for (int i = 0; i < _uuidCardinality; ++i) {
      uuidSet[i] = genUUID();
    }
    return uuidSet;
  }

  public static String genUUID() {
    String charSet = "0123456789abcd";
    int lengthP = RANDOM.nextInt(100);
    int length = 0;
    if (lengthP < 18) {
      length = 65;
    } else if (lengthP < 36) {
      length = 11;
    } else {
      length = 14;
    }
    String uuid = "";
    for (int i = 0; i < length; ++i) {
      uuid += charSet.charAt(RANDOM.nextInt(charSet.length()) % charSet.length());
    }
    return uuid;
  }

  public String getRandomUUID(String[] uuidSet) {
    int uuidIdx = RANDOM.nextInt(uuidSet.length);
    return uuidSet[uuidIdx];
  }

  public String _categories[] = new String[]{"abc", "efg", "hij"};

  public String getRandomCategory() {
    int idx = RANDOM.nextInt(_categories.length);
    return _categories[idx];
  }

  public int _nums[] = new int[]{1001944, 1003220};

  public int getRandomNum(){
    int idx = RANDOM.nextInt(_nums.length);
    return _nums[idx];
  }


  @Setup
  public void setUp()
      throws Exception {
    String[] UUIDSet = generateUUIDSet();

    for (int i = 0; i < NUM_VALUES; i++) {
      Object[] values = new Object[3];
      values[0] = getRandomUUID(UUIDSet);
      values[1] = getRandomCategory();
      values[2] = getRandomNum();
      _keys[i] = new Key(values);
      Object[] copyValues = new Object[3];
      copyValues[0] = new String(getRandomUUID(UUIDSet));
      copyValues[1] = new String(getRandomCategory());
      copyValues[2] = new Integer(getRandomNum());
      _copyKeys[i] = new Key(copyValues);
      _records[i] = new Record(new Double[]{(_rangeMax - _rangeMin) *  RANDOM.nextDouble()});
    }
    _executorService = Executors.newFixedThreadPool(_numThreads);
  }

  @TearDown
  public void tearDown() {
    _executorService.shutdown();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void concurrentHashMapSharedStringPool()
      throws Exception {
    ConcurrentHashMap<Key, Record> mp = new ConcurrentHashMap<Key, Record>();
    CountDownLatch operatorLatch = new CountDownLatch(_numThreads);

    int threadValSize = NUM_VALUES/ _numThreads + 1;
    for (int i = 0; i < _numThreads; i++) {
      int finalI = i;
      _executorService.submit(() -> {
        for(int j = finalI * threadValSize; j < (finalI + 1) * threadValSize && j < NUM_VALUES; ++j){
          int finalJ = j;
          mp.computeIfPresent(_keys[j], (k, v) -> {
            Double[] existingValues = (Double[]) v.getValues();
            Double[] newValues = (Double[]) _records[finalJ].getValues();
            existingValues[0] = _sumAgg.merge(existingValues[0], newValues[0]);
            return v;
          });
        }
        operatorLatch.countDown();
      });
    }
    operatorLatch.await();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void concurrentHashMapStringCopy()
      throws Exception {
    ConcurrentHashMap<Key, Record> mp = new ConcurrentHashMap<Key, Record>();
    CountDownLatch operatorLatch = new CountDownLatch(_numThreads);

    int threadValSize = NUM_VALUES/ _numThreads + 1;
    for (int i = 0; i < _numThreads; i++) {
      int finalI = i;
      _executorService.submit(() -> {
        for(int j = finalI * threadValSize; j < (finalI + 1) * threadValSize && j < NUM_VALUES; ++j){
          int finalJ = j;
          mp.computeIfPresent(_copyKeys[j], (k, v) -> {
            Double[] existingValues = (Double[]) v.getValues();
            Double[] newValues = (Double[]) _records[finalJ].getValues();
            existingValues[0] = _sumAgg.merge(existingValues[0], newValues[0]);
            return v;
          });
        }
        operatorLatch.countDown();
      });
    }
    operatorLatch.await();
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkHashMap.class.getSimpleName()).build()).run();
  }
}
