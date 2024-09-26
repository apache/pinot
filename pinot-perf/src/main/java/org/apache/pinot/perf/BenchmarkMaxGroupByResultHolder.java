package org.apache.pinot.perf;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkMaxGroupByResultHolder {
  private static QueryContext _queryContext;

  @Setup
  public void setUp() {
    String query = "SELECT COUNT(column1), MAX(column1), COUNT(column2), MAX(column2), COUNT(column3), MAX(column3), "
        + "COUNT(column4), MAX(column4), COUNT(column5), MAX(column5), "
        + "COUNT(column6), MAX(column6), COUNT(column7), MAX(column7), COUNT(column8), MAX(column8), "
        + "COUNT(column9), MAX(column9), COUNT(column10), MAX(column10) "
        + "FROM testTable "
        + "WHERE column1 IN (10, 20, 30, 40, 50) "
        + "AND column2 IN (60, 70, 80, 90, 100) "
        + "AND column3 IN (110, 120, 130, 140, 150) "
        + "AND column4 IN (160, 170, 180, 190, 200) "
        + "AND column5 IN (210, 220, 230, 240, 250) "
        + "AND column6 IN (260, 270, 280, 290, 300) "
        + "AND column7 IN (310, 320, 330, 340, 350) "
        + "AND column8 IN (360, 370, 380, 390, 400) "
        + "AND column9 IN (410, 420, 430, 440, 450) "
        + "AND column10 IN (460, 470, 480, 490, 500) "
        + "GROUP BY column1, column2, column3, column4, column5, column6, column7, column8, column9, column10 "
        + "LIMIT 10;";
     _queryContext = QueryContextConverterUtils.getQueryContext(query);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(BenchmarkMaxGroupByResultHolder.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(3)
        .measurementTime(TimeValue.seconds(10))
        .measurementIterations(10)
        .forks(1)
        .build();

    new Runner(opt).run();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchMaxGroupByResultHolder(Blackhole bh) {
    for (int i = 0; i < 100; i++) {
      bh.consume(_queryContext.getMaxInitialResultHolderCapacity());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void benchMaxGroupByResultHolderOld(Blackhole bh) {
    for (int i = 0; i < 100; i++) {
      bh.consume(_queryContext.getMaxInitialResultHolderCapacityOld());
    }
  }
}