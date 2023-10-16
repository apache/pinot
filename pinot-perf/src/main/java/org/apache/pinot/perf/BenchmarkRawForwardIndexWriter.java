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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.function.LongSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;


@State(Scope.Benchmark)
public class BenchmarkRawForwardIndexWriter {
  // https://en.wikipedia.org/wiki/Wikipedia:About
  private static final String[] WORDS = ("Wikipedia is an online free content encyclopedia project helping create a "
      + "world in which everyone can freely share in the sum of all knowledge. It is supported by the Wikimedia "
      + "Foundation and based on a model of freely editable content. The name \"Wikipedia\" is a blending of the "
      + "words wiki (a technology for creating collaborative websites, from the Hawaiian word wiki, meaning "
      + "\"quick\") and encyclopedia. Wikipedia's articles provide links designed to guide the user to related pages "
      + "with additional information.\n"
      + "\n"
      + "Wikipedia is written collaboratively by largely anonymous volunteers. Anyone with Internet access can write "
      + "and make changes to Wikipedia articles, except in limited cases where editing is restricted to prevent "
      + "further disruption or vandalism.\n"
      + "\n"
      + "Since its creation on January 15, 2001, Wikipedia has grown into the world's largest reference website, "
      + "attracting 1.8 billion unique-device visitors monthly as of August 2021. It currently has more than "
      + "fifty-seven million articles in more than 300 languages, including 6,410,117 articles in English with 125,"
      + "342 active contributors in the past month.\n"
      + "\n"
      + "The fundamental principles by which Wikipedia operates are the five pillars. The Wikipedia community has "
      + "developed many policies and guidelines to improve the encyclopedia; however, it is not a requirement to be "
      + "familiar with them before contributing.\n"
      + "\n"
      + "Anyone is allowed to add or edit words, references, images, and other media here. What is contributed is "
      + "more important than who contributes it. To remain, the content must be free of copyright restrictions and "
      + "contentious material about living people. It must fit within Wikipedia's policies, including being "
      + "verifiable against a published reliable source. Editors' opinions and beliefs and unreviewed research will "
      + "not remain. Contributions cannot damage Wikipedia because the software allows easy reversal of mistakes, and"
      + " many experienced editors are watching to ensure that edits are improvements. Begin by simply clicking the "
      + "Edit button at the top of any editable page!\n"
      + "\n"
      + "Wikipedia is a live collaboration differing from paper-based reference sources in important ways. It is "
      + "continually created and updated, with articles on new events appearing within minutes, rather than months or"
      + " years. Because everybody can help improve it, Wikipedia has become more comprehensive than any other "
      + "encyclopedia. Besides quantity, its contributors work on improving quality, removing or repairing "
      + "misinformation, and other errors. Over time, articles tend to become more comprehensive and balanced. "
      + "However, because anyone can click \"edit\" at any time and add content, any article may contain undetected "
      + "misinformation, errors, or vandalism. Readers who are aware of this can obtain valid information, avoid "
      + "recently added misinformation (see Wikipedia:Researching with Wikipedia), and fix the article.").split(" ");

  private byte[][] _bytes;

  @Param("100000")
  int _records;

  @Param({"UNIFORM(1000,10000)", "EXP(0.001)"})
  String _distribution;
  @Param({"SNAPPY", "LZ4", "ZSTANDARD"})
  ChunkCompressionType _chunkCompressionType;

  @Param("1048576")
  int _maxChunkSize;
  int _maxLength;

  @Setup(Level.Trial)
  public void setup()
      throws IOException {
    FileUtils.forceMkdir(TARGET_DIR);
    SplittableRandom random = new SplittableRandom(42);
    LongSupplier supplier = Distribution.createLongSupplier(42, _distribution);
    _bytes = new byte[_records][];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < _records; i++) {
      int words = (int) supplier.getAsLong();
      for (int j = 0; j < words; j++) {
        sb.append(WORDS[random.nextInt(WORDS.length)]);
      }
      _bytes[i] = sb.toString().getBytes(StandardCharsets.UTF_8);
      _maxLength = Math.max(_maxLength, _bytes[i].length);
      sb.setLength(0);
    }
  }

  private static final File TARGET_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkRawForwardIndex");

  @TearDown(Level.Trial)
  public void deleteDir() {
    FileUtils.deleteQuietly(TARGET_DIR);
  }

  @Setup(Level.Trial)
  public void createFile() {
    _file = new File(TARGET_DIR, UUID.randomUUID().toString());
  }

  private File _file;

  @TearDown(Level.Iteration)
  public void after() {
    if (_file != null) {
      FileUtils.deleteQuietly(_file);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void writeV4(BytesCounter counter)
      throws IOException {
    try (
        VarByteChunkForwardIndexWriterV4 writer = new VarByteChunkForwardIndexWriterV4(_file, _chunkCompressionType,
            _maxChunkSize)) {
      for (int i = 0; i < _records; i++) {
        writer.putBytes(_bytes[i]);
      }
    }
    counter._bytes += _file.length();
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  public void writeV3(BytesCounter counter)
      throws IOException {
    try (VarByteChunkForwardIndexWriter writer = new VarByteChunkForwardIndexWriter(_file, _chunkCompressionType,
        _records, _maxChunkSize / _maxLength, _maxLength, 3)) {
      for (int i = 0; i < _records; i++) {
        writer.putBytes(_bytes[i]);
      }
    }
    counter._bytes += _file.length();
  }
}
