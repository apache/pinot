<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# JUnit (Jupiter) in Apache Pinot

Pinot runs its tests on the **JUnit Platform**. The platform is engine-pluggable, and
Pinot enables two engines at once:

- **JUnit Jupiter** — runs new tests written with the JUnit Jupiter API. Pinot pins the
  **JUnit 6** BOM; "JUnit 5" is the same Jupiter programming model under the pre-unified
  version numbers.
- **TestNG engine** (`org.junit.support:testng-engine`) — runs the existing TestNG suite
  unchanged, analogous to how JUnit Vintage runs JUnit 4 tests.

Both engines run in a single Surefire execution (Surefire's `junit-platform` provider).
**You do not need to migrate existing TestNG tests.** They keep their TestNG annotations
and `org.testng.Assert` assertions and run as-is. Write *new* tests in JUnit Jupiter.

## How it is wired (root `pom.xml`)

- The JUnit BOM (`org.junit:junit-bom`, unified version since JUnit 6) is imported in
  `dependencyManagement`; `junit-jupiter`, `testng-engine`, and `junit-platform-launcher`
  are inherited by every module as test-scoped dependencies.
- Surefire has **no explicit provider** dependency — it auto-selects the `junit-platform`
  provider because a JUnit Platform engine is on the test classpath. The launcher is on the
  test classpath so it stays version-aligned with the engines inside the forked JVM.
- TestNG `@Test(groups = "...")` are exposed to the platform as **tags**. Group-based
  selection therefore uses Surefire `<groups>` / `<excludedGroups>` (e.g. the controller
  module splits the `stateless` group into a separate execution). `testng.xml` suite files
  are **not** supported by the engine and are not used.

## Writing a new Jupiter test

Use the `org.junit.jupiter.api` API. Reference example:
`pinot-spi/src/test/java/org/apache/pinot/spi/utils/BooleanUtilsTest.java`.

| Need | TestNG | JUnit Jupiter |
|------|--------|---------------|
| Test method | `@Test` (`org.testng.annotations`) | `@Test` (`org.junit.jupiter.api`) |
| Setup/teardown per method | `@BeforeMethod` / `@AfterMethod` | `@BeforeEach` / `@AfterEach` |
| Setup/teardown per class | `@BeforeClass` / `@AfterClass` | `@BeforeAll` / `@AfterAll` (static) |
| Data-driven | `@DataProvider` + `dataProvider=` | `@ParameterizedTest` + `@ValueSource` / `@MethodSource` / `@CsvSource` |
| Expected exception | `@Test(expectedExceptions = X.class)` | `assertThrows(X.class, () -> ...)` |
| Disable | `@Test(enabled = false)` | `@Disabled` |
| Order | `@Test(priority = n)` | `@TestMethodOrder` + `@Order(n)` |
| Group/tag | `@Test(groups = "g")` | `@Tag("g")` |
| Conditional skip | `throw new SkipException(...)` | `Assumptions.assumeTrue(...)` |
| Mockito | `MockitoAnnotations.openMocks` | `@ExtendWith(MockitoExtension.class)` (add `org.mockito:mockito-junit-jupiter`) |

Conventions:
- Prefer **AssertJ** (`assertThat(actual)...`, already available) or
  `org.junit.jupiter.api.Assertions` for new tests.
- Do **not** mix TestNG and Jupiter annotations in the same class.

### One footnote on assertions (only matters if you *rewrite* a TestNG test)

JUnit's `assertEquals(expected, actual)` takes arguments in the **opposite order** from
TestNG's `assertEquals(actual, expected)`. This is irrelevant for tests left as TestNG
(they keep TestNG's `Assert`). It only matters when converting a test to Jupiter — flip the
argument order, or switch to AssertJ which sidesteps the ambiguity.

## Optionally converting a module to Jupiter

Converting is opt-in and should be done per-module in its own PR. Mechanics: swap the
annotations and assertions per the table above, mind the `assertEquals` arg order, and drop
the module's own `org.testng:testng` dependency once no TestNG test remains in it.
