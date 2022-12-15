package org.apache.pinot.perf;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

  private class Value {
    public String _uuid;
    public String _category;
    public int _num;
  };

  private final Value[] _values = new Value[NUM_VALUES];
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
      Value value = new Value();
      value._uuid = getRandomUUID(UUIDSet);
      value._category = getRandomCategory();
      value._num = getRandomNum();
      _values[i] = value;
    }
    _executorService = Executors.newFixedThreadPool(_numThreads);
  }

  @TearDown
  public void tearDown() {
    _executorService.shutdown();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public void concurrentHashMapPut()
      throws Exception {
    ConcurrentHashMap<Value, Integer> mp = new ConcurrentHashMap<Value, Integer>();
    CountDownLatch operatorLatch = new CountDownLatch(_numThreads);
    int threadValSize = _values.length / _numThreads + 1;
    for (int i = 0; i < _numThreads; i++) {
      int finalI = i;
      _executorService.submit(() -> {
        for(int j = finalI * threadValSize; j < (finalI + 1) * threadValSize && j < _values.length; ++j){
          mp.put(_values[finalI], 1);
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
