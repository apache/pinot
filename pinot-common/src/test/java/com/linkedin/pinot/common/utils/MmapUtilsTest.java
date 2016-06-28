/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.io.File;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for MmapUtils.
 */
public class MmapUtilsTest {
  @Test
  public void testAllocationContext() {
    // No file
    assertEquals(new MmapUtils.AllocationContext("potato", MmapUtils.AllocationType.MMAP).getContext(), "potato");
    assertEquals(new MmapUtils.AllocationContext(null, "potato", MmapUtils.AllocationType.MMAP).getContext(), "potato");

    // With various paths
    assertEquals(
        new MmapUtils.AllocationContext(new File("a"), "potato", MmapUtils.AllocationType.MMAP).getContext(),
        "a (potato)");
    assertEquals(
        new MmapUtils.AllocationContext(new File("/a"), "potato", MmapUtils.AllocationType.MMAP).getContext(),
        "/a (potato)");
    assertEquals(
        new MmapUtils.AllocationContext(new File("/a/b"), "potato", MmapUtils.AllocationType.MMAP).getContext(),
        "/a/b (potato)");
    assertEquals(
        new MmapUtils.AllocationContext(new File("/a/b/c"), "potato", MmapUtils.AllocationType.MMAP).getContext(),
        "/a/b/c (potato)");
    assertEquals(
        new MmapUtils.AllocationContext(new File("/a/b/c/d"), "potato", MmapUtils.AllocationType.MMAP).getContext(),
        "/a/b/c/d (potato)");
  }
}