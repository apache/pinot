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
package org.apache.pinot.spotbugs.plugin;

import edu.umd.cs.findbugs.BugCollection;
import edu.umd.cs.findbugs.test.SpotBugsRule;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcher;
import edu.umd.cs.findbugs.test.matcher.BugInstanceMatcherBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;

import static edu.umd.cs.findbugs.test.CountMatcher.containsExactly;
import static org.junit.Assert.assertThat;


public class ShadedTypesUsedOnSpiDetectorTest {
  @Rule
  public SpotBugsRule _spotbugs = new SpotBugsRule();

  @Test
  public void testGoodCase() {
    Path path =
        Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'), "GoodCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(0, bugTypeMatcher));
  }

  @Test
  public void testBadStaticArgCase() {
    Path path = Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'),
        "BadStaticArgCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
  }

  @Test
  public void testBadStaticResultCase() {
    Path path = Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'),
        "BadStaticResultCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
  }

  @Test
  public void testBadVirtualResultCase() {
    Path path = Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'),
        "BadVirtualResultCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
  }

  @Test
  public void testBadVirtualArgCase() {
    Path path = Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'),
        "BadVirtualArgCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
  }

  @Test
  public void testBadSpecialCase() {
    Path path = Paths.get("target/test-classes", "org.apache.pinot.spotbugs.plugin".replace('.', '/'),
        "BadSpecialCase.class");
    BugCollection bugCollection = _spotbugs.performAnalysis(path);

    BugInstanceMatcher bugTypeMatcher =
        new BugInstanceMatcherBuilder().bugType(ShadedTypesUsedOnSpiDetector.BUG_TYPE).build();
    assertThat(bugCollection, containsExactly(1, bugTypeMatcher));
  }
}
