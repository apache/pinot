package org.apache.pinot.perf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.pinot.segment.local.utils.nativefst.mutablefst.MutableFST;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;

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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 30)
@Measurement(iterations = 5, time = 30)
@Fork(1)
@State(Scope.Benchmark)
public class BenchmarkMutableFST {
  @Param({"q.[aeiou]c.*", ".*a", "b.*", ".*", ".*ated", ".*ba.*"})
  public String _regex;

  private MutableFST _mutableFST;
  private org.apache.lucene.util.fst.FST _fst;

  @Setup
  public void setUp()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("data/words.txt"))))) {
      String currentWord;
      int i = 0;
      while ((currentWord = bufferedReader.readLine()) != null) {
        _mutableFST.addPath(currentWord, i);
        input.put(currentWord, i++);
      }
    }

    _fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(input);
  }

  @Benchmark
  public void testMutableRegex(Blackhole blackhole) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch(_regex, _mutableFST, writer::add);
    blackhole.consume(writer.get());
  }

  @Benchmark
  public void testLuceneRegex(Blackhole blackhole)
      throws IOException {
    blackhole.consume(org.apache.pinot.segment.local.utils.fst.RegexpMatcher.regexMatch(_regex, _fst));
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkFST.class.getSimpleName()).build()).run();
  }
}
