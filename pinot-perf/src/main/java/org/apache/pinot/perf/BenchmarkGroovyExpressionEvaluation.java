/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.perf;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
public class BenchmarkGroovyExpressionEvaluation {

  private final GroovyClassLoader _groovyClassLoader = new GroovyClassLoader();
  private final Random _random = new Random();

  private String _concatScriptText;
  private GroovyCodeSource _concatCodeSource;
  private String _maxScriptText;
  private GroovyCodeSource _maxCodeSource;

  private Binding _concatBinding;
  private Script _concatScript;
  private Script _concatGCLScript;
  private Binding _maxBinding;
  private Script _maxScript;
  private Script _maxGCLScript;

  @Setup
  public void setup()
      throws IllegalAccessException, InstantiationException {
    _concatScriptText = "firstName + ' ' + lastName";
    _concatBinding = new Binding();
    _concatScript = new GroovyShell(_concatBinding).parse(_concatScriptText);
    _concatCodeSource = new GroovyCodeSource(_concatScriptText, Math.abs(_concatScriptText.hashCode()) + ".groovy",
        GroovyShell.DEFAULT_CODE_BASE);
    _concatGCLScript = (Script) _groovyClassLoader.parseClass(_concatCodeSource).newInstance();

    _maxScriptText = "longList.max{ it.toBigDecimal() }";
    _maxBinding = new Binding();
    _maxScript = new GroovyShell(_maxBinding).parse(_maxScriptText);
    _maxCodeSource = new GroovyCodeSource(_maxScriptText, Math.abs(_maxScriptText.hashCode()) + ".groovy",
        GroovyShell.DEFAULT_CODE_BASE);
    _maxGCLScript = (Script) _groovyClassLoader.parseClass(_maxCodeSource).newInstance();
  }

  private String getFirstName() {
    return RandomStringUtils.randomAlphabetic(10);
  }

  private String getLastName() {
    return RandomStringUtils.randomAlphabetic(20);
  }

  private List<String> getLongList() {
    int listLength = _random.nextInt(100) + 10;
    List<String> longList = new ArrayList<>(listLength);
    for (int i = 0; i < listLength; i++) {
      longList.add(String.valueOf(_random.nextInt()));
    }
    return longList;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void javaConcat() {
    getFullNameJava(getFirstName(), getLastName());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groovyShellConcat() {
    getFullNameGroovyShell(getFirstName(), getLastName());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groovyCodeSourceConcat() {
    getFullNameGroovyCodeSource(getFirstName(), getLastName());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void javaMax() {
    List<String> longList = getLongList();
    getMaxJava(longList);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groovyShellMax() {
    List<String> longList = getLongList();
    getMaxGroovyShell(longList);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groovyCodeSourceMax() {
    List<String> longList = getLongList();
    getMaxGroovyCodeSource(longList);
  }

  private String getFullNameJava(String firstName, String lastName) {
    return String.join(" ", firstName, lastName);
  }

  private Object getFullNameGroovyShell(String firstName, String lastName) {
    _concatBinding.setVariable("firstName", firstName);
    _concatBinding.setVariable("lastName", lastName);
    return _concatScript.run();
  }

  private Object getFullNameGroovyCodeSource(String firstName, String lastName) {
    _concatBinding.setVariable("firstName", firstName);
    _concatBinding.setVariable("lastName", lastName);
    _concatGCLScript.setBinding(_concatBinding);
    return _concatGCLScript.run();
  }

  private int getMaxJava(List<String> longList) {
    int maxInt = Integer.MIN_VALUE;
    for (String value : longList) {
      int number = Integer.parseInt(value);
      if (number > maxInt) {
        maxInt = number;
      }
    }
    return maxInt;
  }

  private Object getMaxGroovyShell(List<String> longList) {
    _maxBinding.setVariable("longList", longList);
    return _maxScript.run();
  }

  private Object getMaxGroovyCodeSource(List<String> longList) {
    _maxBinding.setVariable("longList", longList);
    _maxGCLScript.setBinding(_maxBinding);
    return _maxGCLScript.run();
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkGroovyExpressionEvaluation.class.getSimpleName())
        .warmupTime(TimeValue.seconds(10)).warmupIterations(1).measurementTime(TimeValue.seconds(30))
        .measurementIterations(3).forks(1);
    new Runner(opt.build()).run();
  }
}
