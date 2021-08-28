package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
  public void testRegex1(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("q.[aeiou]c.*", fsaStore.fsa, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testRegex2(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("a.*",  fsaStore.fsa, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testRegex3(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("b.*", fsaStore.fsa, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testRegex4(FSAStore fsaStore, Blackhole blackhole) {
    regexQueryNrHits("~#", fsaStore.fsa, blackhole);
  }

  @State(Scope.Thread)
  public static class FSAStore {
    private static byte[][] inputData;
    public static FSA fsa;

    public FSAStore() {

      if (fsa != null) {
        return;
      }

      Set<String> inputStrings = new HashSet<>();
      InputStream fileInputStream = null;
      InputStreamReader inputStreamReader = null;
      BufferedReader bufferedReader = null;

      File directory = new File("pinot-fsa/src/test/resources/data/cocacorpus/");

      try {
        for (final File fileEntry : directory.listFiles()) {
          fileInputStream = new FileInputStream(fileEntry);
          inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
          bufferedReader = new BufferedReader(inputStreamReader);

          String currentLine;
          while ((currentLine = bufferedReader.readLine()) != null) {
            String[] tmp = currentLine.split(" ");    //Split space
            for (String currentWord : tmp) {
              inputStrings.add(currentWord);
            }
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }

      //TODO: atri
      System.out.println("WORDCOUNT IS " + inputStrings.size());

      inputData = convertToBytes(inputStrings);

      Arrays.sort(inputData, FSABuilder.LEXICAL_ORDERING);

      final int min = 200;
      final int max = 400;

      FSABuilder fsaBuilder = new FSABuilder();

      for (byte[] currentArray : inputData) {
        fsaBuilder.add(currentArray, 0, currentArray.length, (int) (Math.random() * (max - min + 1) + min));
      }

      fsa = fsaBuilder.complete();
    }

    private static byte[][] convertToBytes(Set<String> strings) {
      byte[][] data = new byte[strings.size()][];

      Iterator<String> iterator = strings.iterator();

      int i = 0;
      while (iterator.hasNext()) {
        String string = iterator.next();
        data[i] = string.getBytes(Charset.defaultCharset());
        i++;
      }
      return data;
    }
  }

  /**
   * Return all matches for given regex
   */
  private void regexQueryNrHits(String regex, FSA fsa,  Blackhole blackhole) {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fsa);

    blackhole.consume(resultList);
  }
}
