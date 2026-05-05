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
package org.apache.pinot.verifier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.verifier.checks.Check;
import org.apache.pinot.verifier.checks.InputFormatCheck;
import org.apache.pinot.verifier.checks.MetricsFactoryCheck;
import org.apache.pinot.verifier.checks.PinotFsCheck;
import org.apache.pinot.verifier.checks.StreamConsumerCheck;


/**
 * Standalone verifier for plugin loading. Designed to be launched as a separate JVM whose
 * classpath is just {@code lib/pinot-all.jar} (i.e. the production layout) with
 * {@code -Dplugins.dir=plugins/} set, so realm isolation is genuinely exercised rather than
 * masked by Maven's all-plugins-on-classpath test layout.
 *
 * <p>For each known plugin type the verifier exercises the same factory API a real Pinot
 * service uses (e.g. {@code RecordReaderFactory.getRecordReaderByClass}, not a shortcut). A
 * pass means the plugin's class loads, instantiates with its no-arg constructor, and where
 * cheap to test, accepts {@code init(...)} or equivalent. It does <strong>not</strong>
 * connect to external systems (Kafka brokers, S3 buckets) — that's what integration tests are
 * for.</p>
 *
 * <h2>Flags</h2>
 * <ul>
 *   <li>{@code --check <comma-sep types>} — restrict to plugin types: any of
 *       {@code input-format}, {@code stream}, {@code fs}, {@code metrics}. Default:
 *       {@code all}.</li>
 *   <li>{@code --plugin <fqcn>} — verify only the named class (across all checks).</li>
 *   <li>{@code --strict-realm} — pass {@code pluginName:fqcn} to PluginManager instead of
 *       bare fqcn. Bare-fqcn lookup goes through the realm walk added by apache/pinot#18386;
 *       strict-realm form works on both old and new code.</li>
 *   <li>{@code --verbose} — for each loaded class, print {@code getProtectionDomain()
 *       .getCodeSource().getLocation()} so the operator can see whether the class came from
 *       {@code lib/pinot-all.jar} (system classloader path) or from a plugin realm under
 *       {@code plugins/}.</li>
 * </ul>
 *
 * <p>Exit code: 0 if every selected check passes, 1 otherwise.</p>
 */
public final class PluginVerifier {
  private final Options _options;

  private PluginVerifier(Options options) {
    _options = options;
  }

  public static void main(String[] args) {
    Options options;
    try {
      options = parseArgs(args);
    } catch (IllegalArgumentException e) {
      System.err.println("Argument error: " + e.getMessage());
      System.err.println();
      System.err.println(usage());
      System.exit(2);
      return;
    }
    if (options.help) {
      System.out.println(usage());
      System.exit(0);
      return;
    }
    int rc = new PluginVerifier(options).run();
    System.exit(rc);
  }

  /**
   * Returns 0 when every selected check reports pass, 1 otherwise.
   */
  public int run() {
    Map<String, Check> registry = new LinkedHashMap<>();
    registry.put("input-format", new InputFormatCheck());
    registry.put("fs", new PinotFsCheck());
    registry.put("stream", new StreamConsumerCheck());
    registry.put("metrics", new MetricsFactoryCheck());

    Set<String> selectedTypes;
    if (_options.checks.contains("all")) {
      selectedTypes = registry.keySet();
    } else {
      selectedTypes = _options.checks;
      for (String type : selectedTypes) {
        if (!registry.containsKey(type)) {
          System.err.println("Unknown check type: " + type);
          System.err.println("Known types: " + registry.keySet());
          return 2;
        }
      }
    }

    int pluginsDirsAvailable = describePluginsDir();
    if (pluginsDirsAvailable == 0) {
      System.err.println("WARNING: -Dplugins.dir is not set or no plugin directories exist. "
          + "The verifier will only exercise classes available on the system classloader.");
    }
    PluginManager pluginManager = PluginManager.get();

    CheckContext context = new CheckContext(pluginManager, _options);

    int totalPass = 0;
    int totalFail = 0;
    for (String type : selectedTypes) {
      Check check = registry.get(type);
      System.out.println();
      System.out.println("== " + check.name() + " ==");
      Check.Outcome outcome = check.run(context);
      totalPass += outcome.passed();
      totalFail += outcome.failed();
    }

    System.out.println();
    System.out.println("Summary: " + totalPass + " passed, " + totalFail + " failed");
    return totalFail == 0 ? 0 : 1;
  }

  private int describePluginsDir() {
    String configured = System.getProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME);
    if (configured == null) {
      return 0;
    }
    System.out.println("plugins.dir = " + configured);
    int count = 0;
    for (String dir : configured.split(";")) {
      java.io.File f = new java.io.File(dir);
      if (f.isDirectory()) {
        count++;
        if (_options.verbose) {
          System.out.println("  " + dir + " exists");
        }
      } else {
        System.err.println("  WARNING: " + dir + " does not exist");
      }
    }
    return count;
  }

  private static Options parseArgs(String[] args) {
    Options options = new Options();
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--help":
        case "-h":
          options.help = true;
          break;
        case "--check":
          options.checks = new LinkedHashSet<>(Arrays.asList(requireValue(args, ++i).split(",")));
          break;
        case "--plugin":
          options.singlePluginFqcn = requireValue(args, ++i);
          break;
        case "--strict-realm":
          options.strictRealm = true;
          break;
        case "--verbose":
        case "-v":
          options.verbose = true;
          break;
        default:
          throw new IllegalArgumentException("Unknown flag: " + args[i]);
      }
    }
    return options;
  }

  private static String requireValue(String[] args, int idx) {
    if (idx >= args.length) {
      throw new IllegalArgumentException("Missing value for flag " + args[idx - 1]);
    }
    return args[idx];
  }

  private static String usage() {
    return "Usage: java -cp lib/* -Dplugins.dir=plugins/ \\\n"
        + "         org.apache.pinot.verifier.PluginVerifier [flags]\n"
        + "\n"
        + "Flags:\n"
        + "  --check <types>       Comma-separated subset of: input-format, stream, fs, metrics.\n"
        + "                        Default: all\n"
        + "  --plugin <fqcn>       Run only the verifier(s) targeting the named FQCN.\n"
        + "  --strict-realm        Use pluginName:FQCN form instead of bare FQCN. The bare form\n"
        + "                        relies on the realm walk added by apache/pinot#18386; the\n"
        + "                        strict form works on both old and new code.\n"
        + "  --verbose, -v         Print per-class CodeSource so you can see whether each impl\n"
        + "                        came from pinot-all.jar or a plugin realm.\n"
        + "  --help, -h            Show this message.";
  }

  /** Parsed CLI options. Package-private so {@link CheckContext} can read them. */
  static final class Options {
    Set<String> checks = new LinkedHashSet<>(List.of("all"));
    String singlePluginFqcn;
    boolean strictRealm;
    boolean verbose;
    boolean help;
  }

  /** Holder passed to each {@link Check}. */
  public static final class CheckContext {
    private final PluginManager _pluginManager;
    private final Options _options;

    CheckContext(PluginManager pluginManager, Options options) {
      _pluginManager = pluginManager;
      _options = options;
    }

    public PluginManager pluginManager() {
      return _pluginManager;
    }

    public boolean verbose() {
      return _options.verbose;
    }

    public boolean strictRealm() {
      return _options.strictRealm;
    }

    public boolean targets(String fqcn) {
      return _options.singlePluginFqcn == null || _options.singlePluginFqcn.equals(fqcn);
    }

    /** Resolve a plugin FQCN through the configured strict / non-strict path. */
    public Object createInstance(String pluginName, String fqcn) throws Exception {
      if (_options.strictRealm) {
        return _pluginManager.createInstance(pluginName, fqcn);
      }
      return _pluginManager.createInstance(fqcn);
    }

    public Class<?> loadClass(String pluginName, String fqcn) throws ClassNotFoundException {
      if (_options.strictRealm) {
        return _pluginManager.loadClass(pluginName, fqcn);
      }
      return _pluginManager.loadClass(fqcn);
    }

    public List<String> describeCodeSource(Class<?> c) {
      List<String> out = new ArrayList<>(2);
      try {
        var cs = c.getProtectionDomain().getCodeSource();
        out.add("source = " + (cs == null ? "<bootstrap>" : cs.getLocation()));
      } catch (RuntimeException e) {
        out.add("source = <unavailable: " + e.getClass().getSimpleName() + ">");
      }
      out.add("classloader = " + c.getClassLoader());
      return out;
    }
  }
}
