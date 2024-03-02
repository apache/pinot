package org.apache.pinot.perf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@Fork
//@BenchmarkMode(Mode.SampleTime)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class BenchmarkModeAggregation extends BaseQueryBenchmark {
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(BenchmarkModeAggregation.class.getSimpleName())
//        .addProfiler(GCProfiler.class)
        .build();

    new Runner(opt).run();
  }

  @Param({ "true", "false" })
  public boolean _nullHandling;
  @Param({"0", "1", "2", "3", "4", "5"})
  public int _impl;
  @Param({
      "select mode(value, 'MIN', %s) from benchmark",
      "select mode(valueDict, 'MIN', %s) from benchmark"
  })
  public String _aQueryTemplate;
  private String _query;

  @Override
  protected int getRowsPerSegment() {
    return 10000;
  }

  @Override
  protected int getSegmentsPerServer() {
    return 2;
  }

  @Override
  protected List<IntFunction<Object>> createSuppliers() {
    ArrayList<IntFunction<Object>> result = new ArrayList<>(2);

    result.add((row) -> row % 10);

    result.add(periodicNulls(1, 127, true, longGenerator("EXP(0.5)")));
    result.add(periodicNulls(1, 127, true, longGenerator("EXP(0.5)")));

    return result;
  }

  @Override
  protected Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("benchmark")
        .addDimensionField("id", FieldSpec.DataType.INT)
        .addDimensionField("value", FieldSpec.DataType.LONG)
        .addDimensionField("valueDict", FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  protected TableConfig createTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("benchmark")
        .setNoDictionaryColumns(Collections.singletonList("value"))
        .build();
  }

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    init();
    _scenarioBuilder.setup(_nullHandling);
    _query = String.format(_aQueryTemplate, Integer.toString(_impl));
  }

  @TearDown(Level.Trial)
  public void tearDown()
      throws IOException {
    super.tearDown();
  }

  @Benchmark
  public void test(Blackhole bh) {
    _scenarioBuilder.consumeQuery(_query, bh);
  }
}
