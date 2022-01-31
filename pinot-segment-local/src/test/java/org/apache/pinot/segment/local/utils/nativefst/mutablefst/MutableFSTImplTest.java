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
package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils.regexQueryNrHitsForRealTimeFST;
import static org.testng.AssertJUnit.assertEquals;


public class MutableFSTImplTest {
  private MutableFST _fst;

  @BeforeClass
  public void setUp()
      throws Exception {
    _fst = new MutableFSTImpl();

    String regexTestInputString =
        "the quick brown fox jumps over the lazy ???" + "dog dddddd 493432 49344 [foo] 12.3 uick \\foo\\";
    String[] splitArray = regexTestInputString.split("\\s+");

    for (String currentValue : splitArray) {
      _fst.addPath(currentValue, -1);
    }
  }

  @Test
  public void shouldCompactNulls1() {
    List<Integer> listGood = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9);
    List<Integer> listBad = Lists.newArrayList(null, 1, 2, null, 3, 4, null, 5, 6, null, 7, 8, 9, null);
    MutableFSTImpl.compactNulls((ArrayList) listBad);
    assertEquals(listGood, listBad);
  }

  @Test
  public void shouldCompactNulls2() {
    ArrayList<Integer> listBad = (ArrayList) Lists.newArrayList(1);
    MutableFSTImpl.compactNulls(listBad);
    assertEquals(Lists.newArrayList(1), listBad);
  }


  @Test
  public void testRegexMatcherPrefix() {
    MutableFST fst = new MutableFSTImpl();

    fst.addPath("he", 127);
    fst.addPath("hp", 136);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch("h.*", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegexMatcherSuffix() {
    MutableFST fst = new MutableFSTImpl();

    fst.addPath("aeh", 127);
    fst.addPath("pfh", 136);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch(".*h", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegexMatcherSuffix2() {
    MutableFST fst = new MutableFSTImpl();

    fst.addPath("hello-world", 12);
    fst.addPath("hello-world123", 21);
    fst.addPath("still", 123);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch(".*123", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 1);

    writer.reset();

    RealTimeRegexpMatcher.regexMatch(".till", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 1);
  }

  @Test
  public void testRegexMatcherMatchAny() {
    MutableFST fst = new MutableFSTImpl();

    fst.addPath("hello-world", 12);
    fst.addPath("hello-world123", 21);
    fst.addPath("still", 123);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch("hello.*123", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 1);

    writer.reset();
    RealTimeRegexpMatcher.regexMatch("hello.*", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegexMatcherMatchQuestionMark() {
    MutableFST fst = new MutableFSTImpl();

    fst.addPath("car", 12);
    fst.addPath("cars", 21);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RealTimeRegexpMatcher.regexMatch("cars?", fst, writer::add);

    Assert.assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegex1() {
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("q.[aeiou]c.*", _fst), 1);
  }

  @Test
  public void testRegex2() {
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST(".[aeiou]c.*", _fst), 1);
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("q.[aeiou]c.", _fst), 1);
  }

  @Test
  public void testCharacterClasses() {
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("\\d*", _fst), 1);
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("\\d{6}", _fst), 1);
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("[a\\d]{6}", _fst), 1);
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("\\d{2,7}", _fst), 1);
    Assert.assertEquals(regexQueryNrHitsForRealTimeFST("\\d{4}", _fst), 0);
  }
}
