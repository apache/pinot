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
package org.apache.pinot.query.planner.spi;

import java.util.List;
import org.apache.calcite.plan.RelOptRule;


/// Plugin SPI for customizing the multi-stage planner's Calcite rule sets.
///
/// Implementations are discovered via [java.util.ServiceLoader]. Declare your
/// customizer in
/// `META-INF/services/org.apache.pinot.query.planner.spi.RuleSetCustomizer`
/// inside the plugin JAR. Implementations must have a public no-arg constructor.
///
/// [org.apache.pinot.query.planner.rules.PinotRuleSet#loadFromServiceLoader()] first discovers
/// application-classpath customizers, then iterates every plugin classloader registered with
/// [org.apache.pinot.spi.plugin.PluginManager]. Plugin JARs must be loaded
/// before [org.apache.pinot.query.planner.rules.PinotRuleSet#defaultInstance()] is first called;
/// the singleton is initialized once and not refreshed.
///
///
/// [org.apache.pinot.query.planner.rules.PinotRuleSet] invokes every discovered customizer once
/// per [Phase] at broker startup. The customizer receives the mutable per-phase rule list
/// and can append, remove, replace, or reorder rules using standard `List`
/// operations. After every customizer has run, `PinotRuleSet` defensively
/// copies each list to an immutable form; mutations to the supplied list
/// after `customize` returns have no effect.
///
/// ### Iteration order
///
/// Customizers run in `ServiceLoader` iteration order — typically classpath
/// order, which is JVM-dependent. The OSS defaults are themselves contributed
/// by [org.apache.pinot.query.planner.rules.DefaultRuleSetCustomizer]; plugin authors that depend
/// on observing the OSS defaults should not assume a specific position. To guarantee ordering
/// across customizers, drop and re-add rules from your own `customize`
/// implementation.
///
/// ### Example
///
/// ```java
/// public class MyPluginRules implements RuleSetCustomizer {
///   @Override public void customize(Phase phase, List<RelOptRule> rules) {
///     if (phase == Phase.BASIC) {
///       rules.add(MyOptimizationRule.INSTANCE);
///       rules.removeIf(r -> "BadOldRule".equals(r.toString()));
///     }
///   }
/// }
/// ```
///
/// And `META-INF/services/org.apache.pinot.query.planner.spi.RuleSetCustomizer`:
/// ```
/// com.example.MyPluginRules
/// ```
///
/// ### Thread safety
///
/// Customizers are invoked single-threaded during
/// [org.apache.pinot.query.planner.rules.PinotRuleSet] construction.
/// Implementations need not be thread-safe and must not retain references to
/// the supplied list — the list is defensively copied after every customizer
/// has run. A customizer that throws aborts construction and propagates the
/// exception out of [org.apache.pinot.query.planner.rules.PinotRuleSet#loadFromServiceLoader()].
public interface RuleSetCustomizer {

  /// Modify the broker's rule list for the given phase. The list is mutable
  /// during this call only; it is defensively copied after every customizer
  /// has run. Implementations may simply return without mutating when the
  /// phase is not one they care about. Implementations must not insert `null`
  /// rules — `PinotRuleSet` will reject the resulting list.
  void customize(Phase phase, List<RelOptRule> rules);
}
