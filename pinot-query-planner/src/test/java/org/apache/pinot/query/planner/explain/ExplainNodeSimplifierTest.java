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
package org.apache.pinot.query.planner.explain;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/**
 * Tests for {@link ExplainNodeSimplifier}, the broker-side logic that groups the per-segment children of a combine
 * explain node.
 *
 * The simplifier must group mergeable segment plans while keeping genuinely different plans as separate groups, and it
 * must never fail when the inputs are not all mergeable (these tests run with assertions enabled, which previously
 * surfaced as an {@code AssertionError} in the all-or-nothing implementation).
 */
public class ExplainNodeSimplifierTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"col"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

  /// A combine node whose title must contain {@code Combine} for the simplifier to act on it.
  private static final String COMBINE_TITLE = "LeafStageCombineOperator";

  private static ExplainedNode leaf(String title) {
    return new ExplainedNode(0, SCHEMA, null, Collections.emptyList(), title, Map.of());
  }

  private static ExplainedNode leaf(String title, Map<String, Plan.ExplainNode.AttributeValue> attributes) {
    return new ExplainedNode(0, SCHEMA, null, Collections.emptyList(), title, attributes);
  }

  private static ExplainedNode combine(PlanNode... children) {
    return new ExplainedNode(0, SCHEMA, null, Arrays.asList(children), COMBINE_TITLE, Map.of());
  }

  /// Wraps a child the way {@code AcquireReleaseColumnsSegmentOperator} does when prefetch is enabled.
  private static ExplainedNode acquireRelease(PlanNode child) {
    return new ExplainedNode(0, SCHEMA, null, Collections.singletonList(child), "AcquireReleaseColumnsSegment",
        Map.of());
  }

  private static Map<String, Plan.ExplainNode.AttributeValue> docsAttr(long totalDocs) {
    // Mimics a DEFAULT (summed) numeric attribute such as totalDocs.
    return new ExplainAttributeBuilder().putLong("totalDocs", totalDocs).build();
  }

  /// Finds the "Alternative" group under {@code combine} whose plan matches the given chain of titles (from the
  /// Alternative's direct child downwards) and returns its {@code segments} count.
  private static long segmentCountFor(ExplainedNode combine, String... innerTitlePath) {
    for (PlanNode input : combine.getInputs()) {
      ExplainedNode alternative = (ExplainedNode) input;
      assertEquals(alternative.getTitle(), "Alternative", "Each group of a multi-plan combine must be an Alternative");
      if (matchesPath(alternative.getInputs().get(0), innerTitlePath)) {
        return alternative.getAttributes().get("segments").getLong();
      }
    }
    throw new AssertionError("No Alternative group matching plan " + Arrays.toString(innerTitlePath));
  }

  private static boolean matchesPath(PlanNode node, String... titlePath) {
    PlanNode current = node;
    for (int i = 0; i < titlePath.length; i++) {
      if (!(current instanceof ExplainedNode) || !((ExplainedNode) current).getTitle().equals(titlePath[i])) {
        return false;
      }
      if (i < titlePath.length - 1) {
        if (current.getInputs().isEmpty()) {
          return false;
        }
        current = current.getInputs().get(0);
      }
    }
    return true;
  }

  @Test
  public void allIdenticalSegmentsCollapseIntoOne() {
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(
        combine(leaf("Scan"), leaf("Scan"), leaf("Scan")));

    ExplainedNode combine = (ExplainedNode) simplified;
    assertEquals(combine.getTitle(), COMBINE_TITLE);
    assertEquals(combine.getInputs().size(), 1, "Identical segments must collapse into a single group");
    assertEquals(((ExplainedNode) combine.getInputs().get(0)).getTitle(), "Scan");
  }

  @Test
  public void identicalSegmentsWithSummedAttributesMerge() {
    // totalDocs is a DEFAULT numeric attribute and must be summed when merging, not block the merge.
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(
        combine(leaf("Scan", docsAttr(10)), leaf("Scan", docsAttr(20)), leaf("Scan", docsAttr(30))));

    ExplainedNode combine = (ExplainedNode) simplified;
    assertEquals(combine.getInputs().size(), 1);
    ExplainedNode merged = (ExplainedNode) combine.getInputs().get(0);
    assertEquals(merged.getAttributes().get("totalDocs").getLong(), 60L);
  }

  @Test
  public void heterogeneousSegmentsAreGroupedWithCounts() {
    // 3 segments use a "Scan" plan and 2 use a "SortedIndexScan" plan. The simplifier must produce 2 groups, not 5
    // separate children (DATA-116) and not fail, and each group must report how many segments fall into it.
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(
        combine(leaf("Scan"), leaf("SortedIndexScan"), leaf("Scan"), leaf("SortedIndexScan"), leaf("Scan")));

    ExplainedNode combine = (ExplainedNode) simplified;
    assertEquals(combine.getInputs().size(), 2, "Distinct segment plans must be grouped, not listed one by one");
    assertEquals(segmentCountFor(combine, "Scan"), 3L);
    assertEquals(segmentCountFor(combine, "SortedIndexScan"), 2L);
  }

  @Test
  public void unmergeableSegmentsDoNotThrowAndAreKeptWithCounts() {
    // Every segment has a distinct plan: nothing merges. The old all-or-nothing implementation hit `assert false`
    // here (an AssertionError under -ea, the deterministic explain failure). It must now keep every plan as its own
    // group, each with a segment count of 1.
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(
        combine(leaf("A"), leaf("B"), leaf("C")));

    ExplainedNode combine = (ExplainedNode) simplified;
    assertEquals(combine.getInputs().size(), 3);
    assertEquals(segmentCountFor(combine, "A"), 1L);
    assertEquals(segmentCountFor(combine, "B"), 1L);
    assertEquals(segmentCountFor(combine, "C"), 1L);
  }

  @Test
  public void acquireReleaseWrappedSegmentsAreGroupedByInnerPlan() {
    // Reproduces the prefetch-enabled shape from DATA-116: each segment is wrapped in an AcquireReleaseColumnsSegment
    // node. Grouping must happen on the inner plan, collapsing identical segments and reporting their counts.
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(
        combine(
            acquireRelease(leaf("Scan")),
            acquireRelease(leaf("Scan")),
            acquireRelease(leaf("Scan")),
            acquireRelease(leaf("SortedIndexScan"))));

    ExplainedNode combine = (ExplainedNode) simplified;
    assertEquals(combine.getInputs().size(), 2,
        "AcquireReleaseColumnsSegment wrappers must be grouped by their inner plan");
    assertEquals(segmentCountFor(combine, "AcquireReleaseColumnsSegment", "Scan"), 3L);
    assertEquals(segmentCountFor(combine, "AcquireReleaseColumnsSegment", "SortedIndexScan"), 1L);
  }

  @Test
  public void segmentCountsSumWhenSimplifiedPlansAreMergedAcrossServers() {
    // Two servers each see the same divergence (some Scan, some SortedIndexScan). After simplifying each server's
    // combine node, merging the two (as the across-server merge does) must keep 2 groups and sum the segment counts.
    ExplainedNode serverA = (ExplainedNode) ExplainNodeSimplifier.simplifyNode(
        combine(leaf("Scan"), leaf("Scan"), leaf("Scan"), leaf("SortedIndexScan"), leaf("SortedIndexScan")));
    ExplainedNode serverB = (ExplainedNode) ExplainNodeSimplifier.simplifyNode(
        combine(leaf("Scan"), leaf("SortedIndexScan"), leaf("SortedIndexScan"), leaf("SortedIndexScan"),
            leaf("SortedIndexScan")));

    ExplainedNode merged = (ExplainedNode) PlanNodeMerger.mergePlans(serverA, serverB, false);

    assertEquals(merged.getInputs().size(), 2);
    assertEquals(segmentCountFor(merged, "Scan"), 4L, "3 + 1 Scan segments across the two servers");
    assertEquals(segmentCountFor(merged, "SortedIndexScan"), 6L, "2 + 4 SortedIndexScan segments across servers");
  }

  @Test
  public void singleSegmentIsReturnedUnchanged() {
    ExplainedNode original = combine(leaf("Scan"));
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(original);
    assertSame(simplified, original, "A combine node with a single input is already simplified");
  }

  @Test
  public void nonCombineNodeRecursesButDoesNotGroupChildren() {
    // A node whose title does not contain "Combine" must keep all its children, only recursing into them.
    ExplainedNode nonCombine = new ExplainedNode(0, SCHEMA, null,
        Arrays.asList(leaf("Scan"), leaf("Scan")), "InstanceResponse", Map.of());
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(nonCombine);
    assertEquals(((ExplainedNode) simplified).getInputs().size(), 2,
        "Non-combine nodes must not group their children");
  }

  @Test
  public void nestedCombineUnderNonCombineIsSimplified() {
    ExplainedNode nested = new ExplainedNode(0, SCHEMA, null,
        Collections.singletonList(combine(leaf("Scan"), leaf("Scan"))), "InstanceResponse", Map.of());
    PlanNode simplified = ExplainNodeSimplifier.simplifyNode(nested);

    assertNotSame(simplified, nested);
    ExplainedNode innerCombine = (ExplainedNode) ((ExplainedNode) simplified).getInputs().get(0);
    assertEquals(innerCombine.getInputs().size(), 1, "Nested combine node must be simplified too");
  }
}
