/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.spec;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.detection.components.RuleBaselineProvider;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AbstractSpecTest {
  @Test
  public void testAbstractSpecMapping() {
    TestSpec spec = AbstractSpec.fromProperties(ImmutableMap.of(), TestSpec.class);
    Assert.assertEquals(spec.getA(), 123);
    Assert.assertEquals(spec.getB(), 456.7);
    Assert.assertEquals(spec.getC(), "default");
  }

  @Test
  public void testAbstractSpecMapping1() {
    TestSpec spec = AbstractSpec.fromProperties(ImmutableMap.of("a", 321), TestSpec.class);
    Assert.assertEquals(spec.getA(), 321);
    Assert.assertEquals(spec.getB(), 456.7);
    Assert.assertEquals(spec.getC(), "default");
  }

  @Test
  public void testAbstractSpecMapping2() {
    RuleBaselineProvider provider = new RuleBaselineProvider();
    TestSpec spec = AbstractSpec.fromProperties(ImmutableMap.of("baselineProvider", provider), TestSpec.class);
    Assert.assertEquals(spec.getA(), 123);
    Assert.assertEquals(spec.getBaselineProvider(), provider);
    Assert.assertEquals(spec.getB(), 456.7);
    Assert.assertEquals(spec.getC(), "default");
  }

  @Test
  public void testAbstractSpecMapping3() {
    TestSpec spec = AbstractSpec.fromProperties(ImmutableMap.of("a", 321, "className", "org.test.Test"), TestSpec.class);
    Assert.assertEquals(spec.getA(), 321);
    Assert.assertEquals(spec.getB(), 456.7);
    Assert.assertEquals(spec.getC(), "default");
  }

}

