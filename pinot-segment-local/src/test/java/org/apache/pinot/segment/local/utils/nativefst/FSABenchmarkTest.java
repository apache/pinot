package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

public class FSABenchmarkTest {

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex1(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("q.[aeiou]c.*", fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex2(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*a",  fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex3(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("b.*", fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex4(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*", fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex5(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*ated", fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex6(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*ba.*", fsaStore.nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex1(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("q.[aeiou]c.*", fsaStore.fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex2(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*a",  fsaStore.fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex3(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("b.*", fsaStore.fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex4(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*", fsaStore.fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex5(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*ated", fsaStore.fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex6(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits(".*ba.*", fsaStore.fst, blackhole);
  }


  @State(Scope.Benchmark)
  public static class FSAStore {
    public static FSA nativeFST;
    public static FST<Long> fst;
    public static boolean initialized;

    public FSAStore() {

      if (initialized) {
        return;
      }

      SortedMap<String, Integer> inputStrings = new TreeMap<>();
      InputStream fileInputStream;
      InputStreamReader inputStreamReader;
      BufferedReader bufferedReader;

      File directory = new File("pinot-segment-local/src/test/resources/data/cocacorpus/");

      int count1 = 0;

      try {
        for (final File fileEntry : directory.listFiles()) {
          fileInputStream = new FileInputStream(fileEntry);
          inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
          bufferedReader = new BufferedReader(inputStreamReader);

          String currentLine;
          while ((currentLine = bufferedReader.readLine()) != null) {
            String[] tmp = currentLine.split("\\s+");    //Split space
            for (String currentWord : tmp) {
              inputStrings.put(currentWord, (int) Math.random());
              count1 = count1 + currentWord.length();
            }
          }
        }

        nativeFST = FSABuilder.buildFSA(inputStrings);
        fst = FSTBuilder.buildFST(inputStrings);

        initialized = true;
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }

      /*
      try {
        SortedMap<String, Integer> inputStrings = new TreeMap<>();
        InputStream fileInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;

        File file = new File("pinot-segment-local/src/test/resources/data/words.txt");

        fileInputStream = new FileInputStream(file);
        inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
        bufferedReader = new BufferedReader(inputStreamReader);

        String currentWord;
        int i = 0;
        while ((currentWord = bufferedReader.readLine()) != null) {
          inputStrings.put(currentWord, i);
          i++;
        }

        fsa = FSABuilder.buildFSA(inputStrings);
        fst = FSTBuilder.buildFST(inputStrings);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }*/
    }
  }

  private void regexQueryNrHits(String regex, FSA fsa,  Blackhole blackhole) {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fsa);

    blackhole.consume(resultList);
  }

  private void regexQueryNrHits(String regex, FST fst,  Blackhole blackhole) {
    try {
      List<Long> resultList = org.apache.pinot.segment.local.utils.fst.RegexpMatcher.regexMatch(regex, fst);

      blackhole.consume(resultList);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
