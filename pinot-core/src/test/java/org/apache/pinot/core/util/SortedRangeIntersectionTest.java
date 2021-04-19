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
package org.apache.pinot.core.util;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.pinot.spi.utils.Pairs;
import org.apache.pinot.spi.utils.Pairs.IntPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for SortedRangeIntersection utility class.
 *
 * We hardcoded or random generated some lists of sorted doc range pairs (inclusive), and inside each list, there is no
 * overlap between two pairs. Then we compare the SortedRangeIntersection results with our simple set based brute force
 * solution results to verify the correctness of SortedRangeIntersection utility class.
 */
public class SortedRangeIntersectionTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedRangeIntersectionTest.class);

  @Test
  public void testSimple() {
    List<IntPair> rangeSet1 = Arrays.asList(Pairs.intPair(0, 4), Pairs.intPair(6, 10));
    List<IntPair> rangeSet2 = Arrays.asList(Pairs.intPair(4, 7), Pairs.intPair(8, 14));
    List<List<IntPair>> newArrayList = Arrays.asList(rangeSet1, rangeSet2);
    List<IntPair> intersectionPairs = SortedRangeIntersection.intersectSortedRangeSets(newArrayList);
    // expected (4,4) (6,10)
    Assert.assertEquals(intersectionPairs.size(), 2);
    Assert.assertEquals(intersectionPairs.get(0), Pairs.intPair(4, 4));
    Assert.assertEquals(intersectionPairs.get(1), Pairs.intPair(6, 10));
  }

  @Test
  public void testComplex() {
    String rangeSet1 = "[[9148,10636], [18560,21885], [29475,32972], [34313,37642], [38008,47157], [50962,53911], "
        + "[59240,68238], [72458,83087], [92235,97593], [100690,103101], [111708,120102], [123212,124718], "
        + "[127544,134012], [134701,141314], [144966,146889], [147206,156680], [163842,168747], [175241,179737], "
        + "[180669,184662], [192538,200004], [207255,211226], [217529,219152], [221319,228532], [230549,236908], "
        + "[237353,240840], [245097,251894], [253228,257447], [263986,268166], [272168,276416], [282756,285555], "
        + "[286030,289848], [293220,303828], [308259,317810], [320830,330498], [337606,345534], [354205,361367], "
        + "[365751,375129], [379830,382548], [390661,399509], [409031,415694], [421748,428011], [436729,442978], "
        + "[443187,448760], [454285,464404], [469128,471735], [475965,478758], [483038,491060], [496477,499410], "
        + "[502719,507364], [511478,515427], [521615,523897], [524251,529600], [530904,536822], [541666,543826], "
        + "[551652,555367], [561244,565874], [573934,582151], [587804,593424], [596533,599490], [601884,605244], "
        + "[610479,618173], [627032,630079], [633582,643323], [648357,658921], [662083,664340], [666519,677174], "
        + "[681524,687223], [693032,696329], [700808,705461], [709573,713092], [722500,732846], [733115,741189], "
        + "[742183,743217], [748442,754700], [760482,768791], [769875,773877], [774153,775538], [778521,781333], "
        + "[781945,791595], [799389,809167], [811769,814445], [824160,831582], [838445,844533], [850597,858212], "
        + "[862638,867759], [877243,887468], [893193,895091], [902608,908295], [911058,915872], [916127,917590], "
        + "[922702,933633], [938082,946932], [953197,956096], [965980,970314], [976357,983182], [983378,991764]]";
    String rangeSet2 = "[[4615,7365], [9048,19300], [25607,33900], [37975,45734], [53195,58803], [64401,72246], "
        + "[76305,84289], [86575,96695], [104465,114232], [121799,124496], [132587,137518], [146406,152055], "
        + "[154808,159304], [165855,168693], [177387,184548], [192275,202089], [204700,215167], [218780,219934], "
        + "[220492,224530], [227195,231541], [233667,241692], [249043,251396], [258494,263095], [271187,272880], "
        + "[279871,287604], [295906,302319], [302575,309352], [318221,320436], [324492,326129], [326623,333708], "
        + "[340839,349361], [356638,361977], [368099,373667], [374773,377525], [378682,380033], [387254,396509], "
        + "[405096,411616], [421132,424029], [426427,435377], [442540,447244], [453501,459620], [462366,471360], "
        + "[473395,484316], [492462,502422], [503755,507454], [507551,510017], [511176,516561], [522063,525977], "
        + "[531770,537861], [540539,542996], [547329,557420], [560641,570186], [570284,578197], [583861,588606], "
        + "[591016,596724], [601714,610872], [614557,622940], [632723,634975], [636710,647340], [647937,657306], "
        + "[666519,671699], [673434,679252], [679505,687724], [695809,697606], [705905,710503], [719044,728326], "
        + "[735262,739796], [748048,753094], [762698,768074], [771762,781103], [786979,789938], [790140,794143], "
        + "[800910,806993], [807930,811850], [818716,827521], [828786,839104], [840596,850617], [851100,858980], "
        + "[863671,874042], [874432,880240], [889917,897380], [897599,904508], [910935,914564], [919538,927762], "
        + "[933690,942122], [951330,959747], [969266,978196], [984965,991648]]";
    String rangeSet3 = "[[3987,12147], [16890,26115], [34621,41095], [41476,44775], [50031,51695], [56539,60977], "
        + "[67558,76247], [83873,86817], [94900,102884], [111390,119073], [127792,138059], [145130,148633], "
        + "[155709,157847], [158118,161503], [164296,165881], [166123,169365], [178304,182866], [191004,193315], "
        + "[198648,199930], [204806,209054], [209177,213999], [214843,219858], [221497,223059], [228746,236624], "
        + "[241482,244679], [254433,255982], [257794,260287], [270207,277022], [278621,286999], [296719,303032], "
        + "[310590,318040], [323645,333283], [336166,341711], [347485,352255], [353348,358126], [362660,368061], "
        + "[376141,381147], [390178,393612], [399003,401626], [402609,412126], [419426,423852], [430009,432359], "
        + "[436647,442494], [446986,453904], [457694,461029], [466339,474088], [483026,486699], [495143,499556], "
        + "[506900,510220], [516325,521843], [523249,528070], [528549,532543], [533418,538998], [547895,549483], "
        + "[554460,555634], [562049,569098], [576463,584537], [588353,590300], [595284,600900], [609006,610452], "
        + "[618857,627533], [633186,637955], [642446,650849], [655326,662913], [663654,673388], [673914,678458], "
        + "[685951,690391], [699505,707103], [715822,718387], [725073,735513], [739306,741642], [750077,752683], "
        + "[759644,768461], [775885,778884], [783314,785772], [792568,802284], [806644,813017], [821962,828876], "
        + "[837176,847172], [854336,864364], [874180,881592], [888633,899508], [906913,907927], [908291,918558], "
        + "[927183,929930], [939548,949063], [951579,957887], [958917,960053], [968720,973220], [978731,989441]]";
    String expectedOutputRangeSet = "[[9148,10636], [18560,19300], [38008,41095], [41476,44775], [67558,68238], "
        + "[94900,96695], [111708,114232], [132587,134012], [134701,137518], [146406,146889], [147206,148633], "
        + "[155709,156680], [165855,165881], [166123,168693], [178304,179737], [180669,182866], [192538,193315], "
        + "[198648,199930], [207255,209054], [209177,211226], [218780,219152], [221497,223059], [230549,231541], "
        + "[233667,236624], [272168,272880], [282756,285555], [286030,286999], [296719,302319], [302575,303032], "
        + "[324492,326129], [326623,330498], [340839,341711], [356638,358126], [379830,380033], [390661,393612], "
        + "[409031,411616], [421748,423852], [446986,447244], [457694,459620], [469128,471360], [483038,484316], "
        + "[496477,499410], [506900,507364], [523249,523897], [524251,525977], [531770,532543], [533418,536822], "
        + "[554460,555367], [562049,565874], [576463,578197], [588353,588606], [596533,596724], [633582,634975], "
        + "[636710,637955], [642446,643323], [648357,650849], [655326,657306], [666519,671699], [673914,677174], "
        + "[685951,687223], [725073,728326], [735262,735513], [739306,739796], [750077,752683], [762698,768074], "
        + "[778521,778884], [800910,802284], [806644,806993], [807930,809167], [811769,811850], [824160,827521], "
        + "[828786,828876], [838445,839104], [840596,844533], [854336,858212], [863671,864364], [877243,880240], "
        + "[893193,895091], [911058,914564], [927183,927762], [939548,942122], [953197,956096], [969266,970314], "
        + "[984965,989441]]";

    List<List<IntPair>> sortedRangeSetList =
        Arrays.asList(constructRangeSet(rangeSet1), constructRangeSet(rangeSet2), constructRangeSet(rangeSet3));
    List<IntPair> expected = constructRangeSet(expectedOutputRangeSet);
    List<IntPair> actual = SortedRangeIntersection.intersectSortedRangeSets(sortedRangeSetList);
    Assert.assertEquals(actual, expected);
  }

  List<IntPair> constructRangeSet(String formattedString) {
    formattedString = formattedString.replace('[', ' ');
    formattedString = formattedString.replace(']', ' ');
    String[] splits = formattedString.split(",");
    int length = splits.length;
    Preconditions.checkState(length % 2 == 0);

    List<IntPair> pairs = new ArrayList<>();
    for (int i = 0; i < length; i += 2) {
      int start = Integer.parseInt(splits[i].trim());
      int end = Integer.parseInt(splits[i + 1].trim());
      pairs.add(Pairs.intPair(start, end));
    }
    return pairs;
  }

  @Test
  public void testRandom() {
    int totalDocs = 1000000;// 1 million docs
    int maxRange = 10000;
    int minRange = 1000;
    long randomSeed = System.currentTimeMillis();
    Random r = new Random(randomSeed);
    int numLists = 3;
    List<List<IntPair>> sortedRangePairsList = new ArrayList<>();
    List<Set<Integer>> rawIdSetList = new ArrayList<>();
    for (int i = 0; i < numLists; i++) {
      List<IntPair> pairs = new ArrayList<>();
      Set<Integer> rawIdSet = new HashSet<>();
      int docId = 0;
      while (docId < totalDocs) {
        int start = docId + r.nextInt(maxRange);
        int end = start + Math.max(minRange, r.nextInt(maxRange));
        if (end < totalDocs) {
          pairs.add(Pairs.intPair(start, end));
          for (int id = start; id <= end; id++) {
            rawIdSet.add(id);
          }
        }
        docId = end + 1;
      }
      sortedRangePairsList.add(pairs);
      rawIdSetList.add(rawIdSet);
    }
    // expected intersection
    List<IntPair> expected = new ArrayList<>();
    int tempStart = -1;
    for (int id = 0; id < totalDocs; id++) {
      boolean foundInAll = true;
      for (int i = 0; i < numLists; i++) {
        if (!rawIdSetList.get(i).contains(id)) {
          foundInAll = false;
          break;
        }
      }
      if (foundInAll) {
        if (tempStart == -1) {
          tempStart = id;
        }
      } else {
        if (tempStart != -1) {
          expected.add(Pairs.intPair(tempStart, id - 1));
          tempStart = -1;
        }
      }
    }
    List<IntPair> actual = SortedRangeIntersection.intersectSortedRangeSets(sortedRangePairsList);

    if (!actual.equals(expected)) {
      LOGGER.error("Actual pairs not equal to expected pairs.");
      LOGGER.error("Actual pairs: {}", actual);
      LOGGER.error("Expected pairs: {}", expected);
      LOGGER.error("Random seed: {}", randomSeed);
      LOGGER.error("Sorted range pairs list: {}", sortedRangePairsList);
      Assert.fail();
    }
  }
}
