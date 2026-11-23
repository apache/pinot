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

import java.io.IOException;
import java.io.InputStream;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.ClassTransform;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.classfile.constantpool.FieldRefEntry;
import java.lang.classfile.constantpool.MemberRefEntry;
import java.lang.classfile.constantpool.PoolEntry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/// Checks an assembled Pinot binary distribution built with the `thin-plugins` profile.
///
/// Usage: `java ThinDistributionCheck.java <thin dist> [--baseline <default dist>] [--known <file>]
/// [--descriptor <pinot-assembly.xml>]`
///
/// Two groups of checks run:
///
/// - Layout invariants of `plugin-libs/` and the `pinot-plugin.classpath` indexes.
/// - Linkage: for each class path that the launchers build, every method and field reference between
///   two jars must resolve against the class that wins on that class path. A reference whose owner
///   class exists but lacks the member is a problem. A reference to a missing class is a problem when
///   it names a relocated package, or, with `--baseline`, when it resolves in the default distribution.
///
/// A problem fails the check unless the default distribution has it too (`--baseline`), or the
/// `--known` file lists its target. The known file holds one target per line, as the report prints
/// it, and `#` comments. It records problems that exist in the default distribution as well, so that
/// CI does not need to build both. A known entry that no longer occurs fails the check, so that the
/// file does not hide a later regression with the same signature.
///
/// The plugin set must match the default distribution (`--baseline`) or the plugin destinations of the
/// default assembly descriptor (`--descriptor`).
///
/// Limits of the linkage model: it does not check access flags, static versus instance members,
/// abstract methods that an implementation lacks, multi-release entries, or `META-INF/services`. It
/// resolves JDK classes against the JDK that runs it. It orders the jars of a directory alphabetically,
/// while the JVM expands a `dir/*` class path entry in file system order.
///
/// The process exits with 1 when a check fails. Requires Java 24 or later (`java.lang.classfile`).
public class ThinDistributionCheck {
  private static final String RELOCATED_PREFIX = "org/apache/pinot/shaded/";
  private static final String INDEX = "pinot-plugin.classpath";
  private static final Pattern VERSIONED_JAR = Pattern.compile("(.+?)-(\\d[^/]*)\\.jar");

  /// The parts of a class that linkage needs. `refs` holds references to classes outside the JDK.
  record ClassInfo(String jar, String superName, List<String> interfaces, Set<String> fields,
      Set<String> methods, List<Ref> refs, Set<String> classRefs) {
  }

  record Ref(String owner, String name, String type, boolean method) {
    String render() {
      return owner.replace('/', '.') + (method ? "." + name + type : "#" + name + ":" + type);
    }
  }

  /// One problem found on one class path, keyed without jar names so that it can be compared
  /// between distributions.
  record Problem(String kind, String fromClass, String target) {
  }

  private final Map<Path, Map<String, ClassInfo>> _jarCache = new HashMap<>();
  private final Map<String, ClassInfo> _jdkCache = new HashMap<>();

  public static void main(String[] args)
      throws Exception {
    Path dist = null;
    Path baseline = null;
    Set<String> known = new HashSet<>();
    Path descriptor = null;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--baseline" -> baseline = Path.of(args[++i]).toAbsolutePath().normalize();
        case "--known" -> Files.readAllLines(Path.of(args[++i])).stream().map(String::strip)
            .filter(l -> !l.isEmpty() && !l.startsWith("#")).forEach(known::add);
        case "--descriptor" -> descriptor = Path.of(args[++i]);
        default -> dist = Path.of(args[i]).toAbsolutePath().normalize();
      }
    }
    if (dist == null) {
      System.err.println("Usage: java ThinDistributionCheck.java <thin dist> [--baseline <default dist>] "
          + "[--known <file>] [--descriptor <pinot-assembly.xml>]");
      System.exit(2);
    }
    ThinDistributionCheck check = new ThinDistributionCheck();
    int failures = check.checkLayout(dist);
    failures += check.checkPluginSet(dist, baseline, descriptor);
    failures += check.checkArtifacts(dist);
    failures += check.checkLinkage(dist, baseline, known);
    System.out.println(failures == 0 ? "\nAll checks passed." : "\n" + failures + " check(s) failed.");
    System.exit(failures == 0 ? 0 : 1);
  }

  // ---------------------------------------------------------------------------------------------
  // Layout invariants
  // ---------------------------------------------------------------------------------------------

  int checkLayout(Path dist)
      throws IOException {
    System.out.println("== Layout of " + dist);
    int failures = 0;
    Path store = dist.resolve("plugin-libs");
    Set<String> storeJars = new TreeSet<>();
    if (Files.isDirectory(store)) {
      try (Stream<Path> s = Files.list(store)) {
        s.filter(p -> p.toString().endsWith(".jar")).forEach(p -> storeJars.add("plugin-libs/" + p.getFileName()));
      }
    }
    Set<String> referenced = new TreeSet<>();
    List<String> errors = new ArrayList<>();
    for (Path pluginDir : pluginDirs(dist.resolve("plugins"))) {
      String rel = dist.relativize(pluginDir).toString();
      Path index = pluginDir.resolve(INDEX);
      if (!Files.exists(index)) {
        errors.add(rel + ": no " + INDEX);
        continue;
      }
      String content = Files.readString(index);
      if (content.contains(";") || content.contains("\\")) {
        errors.add(rel + ": " + INDEX + " uses ';' or '\\', only ':' or newlines and '/' are portable");
      }
      for (String entry : content.split("[:\\n]")) {
        entry = entry.strip();
        if (entry.isEmpty()) {
          continue;
        }
        referenced.add(entry);
        if (!entry.startsWith("plugin-libs/")) {
          errors.add(rel + ": index entry outside plugin-libs/: " + entry);
        } else if (!Files.isRegularFile(dist.resolve(entry))) {
          errors.add(rel + ": index entry does not exist: " + entry);
        }
      }
    }
    for (String jar : storeJars) {
      if (!referenced.contains(jar)) {
        errors.add(jar + ": in plugin-libs/ but no index references it (stale from an earlier build?)");
      }
    }
    // Two versions of one artifact: without a plugin selection both are on the class path, and the
    // one that wins depends on the order of the directory listing.
    Map<String, Set<String>> versions = new TreeMap<>();
    for (String jar : storeJars) {
      Matcher m = VERSIONED_JAR.matcher(jar.substring("plugin-libs/".length()));
      if (m.matches()) {
        versions.computeIfAbsent(groupId(dist.resolve(jar), m.group(1)) + ":" + m.group(1), k -> new TreeSet<>())
            .add(m.group(2));
      }
    }
    versions.forEach((artifact, v) -> {
      if (v.size() > 1) {
        errors.add("plugin-libs/: " + artifact + " present in several versions " + v);
      }
    });
    if (Files.isDirectory(dist.resolve("plugins-external"))) {
      try (Stream<Path> s = Files.walk(dist.resolve("plugins-external"))) {
        s.filter(p -> p.toString().endsWith(".jar"))
            .forEach(p -> errors.add(dist.relativize(p) + ": plugins-external/ jars cannot link against the thin "
                + "pinot-all and must not ship in a thin distribution"));
      }
    }
    System.out.println("plugin-libs/: " + storeJars.size() + " jars, referenced by indexes: " + referenced.size());
    for (String e : errors) {
      System.out.println("  FAIL " + e);
    }
    failures += errors.isEmpty() ? 0 : 1;
    return failures;
  }

  /// The thin distribution must ship the plugin directories of the default distribution, taken from the
  /// baseline distribution or from the default assembly descriptor.
  int checkPluginSet(Path dist, Path baseline, Path descriptor)
      throws IOException {
    Set<String> base = new TreeSet<>();
    if (baseline != null) {
      System.out.println("\n== Plugin set against " + baseline);
      pluginDirs(baseline.resolve("plugins")).forEach(d -> base.add(baseline.relativize(d).toString()));
    } else if (descriptor != null) {
      System.out.println("\n== Plugin set against " + descriptor);
      Matcher m = Pattern.compile("<destName>\\s*(plugins/[^<\\s]+)/[^/<\\s]+\\s*</destName>")
          .matcher(Files.readString(descriptor));
      while (m.find()) {
        base.add(m.group(1));
      }
    } else {
      return 0;
    }
    Set<String> thin = new TreeSet<>();
    pluginDirs(dist.resolve("plugins")).forEach(d -> thin.add(dist.relativize(d).toString()));
    Set<String> missing = new TreeSet<>(base);
    missing.removeAll(thin);
    Set<String> extra = new TreeSet<>(thin);
    extra.removeAll(base);
    missing.forEach(d -> System.out.println("  FAIL only in the default distribution: " + d));
    extra.forEach(d -> System.out.println("  FAIL only in the thin distribution: " + d));
    System.out.println("plugins: " + thin.size() + " thin, " + base.size() + " default");
    return missing.isEmpty() && extra.isEmpty() ? 0 : 1;
  }

  /// Packages in which each patched jar may carry Guava or Jackson classes: the package the original
  /// jar relocates its embedded copy into. Any other Guava or Jackson class means that a new version
  /// of the original moved or added an embedded copy that the patch does not replace.
  private static final Map<String, List<String>> PATCH_PACKAGES = Map.of(
      "pinot-patched-third-party-jackson-core", List.of("software/amazon/awssdk/thirdparty/jackson/"),
      "pinot-patched-hadoop-shaded-guava", List.of("org/apache/hadoop/thirdparty/"),
      "pinot-patched-curator-client", List.of("org/apache/curator/shaded/"),
      "pinot-patched-parquet-jackson", List.of("shaded/parquet/com/fasterxml/jackson/"),
      "pinot-patched-pulsar-client-all", List.of("org/apache/pulsar/shade/"),
      "pinot-patched-reactor-netty-http", List.of());
  private static final Pattern GUAVA_OR_JACKSON = Pattern.compile(
      "(.*/)?(com/google/common|com/google/thirdparty|com/fasterxml/jackson)/.*\\.class");

  /// Checks the Pinot jars that the thin layout adds: every one carries the Pinot LICENSE and NOTICE,
  /// and each patched jar carries Guava and Jackson only in its expected packages.
  int checkArtifacts(Path dist)
      throws IOException {
    System.out.println("\n== Pinot artifacts in plugins/ and plugin-libs/");
    List<String> errors = new ArrayList<>();
    List<Path> pinotJars = new ArrayList<>();
    for (Path dir : List.of(dist.resolve("plugins"), dist.resolve("plugin-libs"))) {
      jars(dir, true).stream().filter(j -> j.getFileName().toString().startsWith("pinot-")).forEach(pinotJars::add);
    }
    for (Path jar : pinotJars) {
      String rel = dist.relativize(jar).toString();
      try (ZipFile zip = new ZipFile(jar.toFile())) {
        for (String legal : List.of("META-INF/LICENSE", "META-INF/NOTICE")) {
          ZipEntry e = zip.getEntry(legal);
          if (e == null) {
            errors.add(rel + ": no " + legal);
          } else if (legal.endsWith("NOTICE")) {
            try (InputStream in = zip.getInputStream(e)) {
              if (!new String(in.readAllBytes()).contains("Apache Pinot")) {
                errors.add(rel + ": " + legal + " is not the Apache Pinot NOTICE");
              }
            }
          }
        }
        String name = jar.getFileName().toString();
        PATCH_PACKAGES.forEach((module, allowed) -> {
          if (!name.startsWith(module + "-")) {
            return;
          }
          Set<String> unexpected = new TreeSet<>();
          zip.stream().map(e -> e.getName().replaceFirst("^META-INF/versions/\\d+/", ""))
              .filter(n -> GUAVA_OR_JACKSON.matcher(n).matches())
              .filter(n -> allowed.stream().noneMatch(n::startsWith))
              .forEach(n -> unexpected.add(n.substring(0, n.lastIndexOf('/'))));
          unexpected.forEach(pkg -> errors.add(rel + ": Guava or Jackson class outside the expected packages: " + pkg));
        });
      }
    }
    errors.forEach(e -> System.out.println("  FAIL " + e));
    System.out.println("Pinot jars checked: " + pinotJars.size());
    return errors.isEmpty() ? 0 : 1;
  }

  /// The groupId from the `pom.properties` of the artifact inside the jar, or `?` when it has none.
  static String groupId(Path jar, String artifactId)
      throws IOException {
    try (ZipFile zip = new ZipFile(jar.toFile())) {
      Pattern p = Pattern.compile("META-INF/maven/([^/]+)/" + Pattern.quote(artifactId) + "/pom\\.properties");
      return zip.stream().map(e -> p.matcher(e.getName())).filter(Matcher::matches).map(m -> m.group(1))
          .findFirst().orElse("?");
    }
  }

  /// A plugin directory is a directory that directly contains a jar, as the launchers assume.
  static List<Path> pluginDirs(Path root)
      throws IOException {
    if (!Files.isDirectory(root)) {
      return List.of();
    }
    try (Stream<Path> s = Files.walk(root)) {
      return s.filter(p -> p.toString().endsWith(".jar")).map(Path::getParent).distinct().sorted().toList();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Linkage
  // ---------------------------------------------------------------------------------------------

  int checkLinkage(Path dist, Path baseline, Set<String> known)
      throws IOException {
    System.out.println("\n== Linkage of " + dist);
    Map<Problem, Set<String>> thin = linkageProblems(dist, new HashSet<>());
    Set<Problem> inBaseline = new HashSet<>();
    Set<String> baselineClasses = new HashSet<>();
    if (baseline != null) {
      System.out.println("   baseline: " + baseline);
      inBaseline.addAll(linkageProblems(baseline, baselineClasses).keySet());
    }
    // Missing classes are grouped by package: one missing optional dependency is one problem.
    record Group(Set<String> targets, Set<String> from, Set<String> classPaths) {
    }
    Map<String, Group> failing = new TreeMap<>();
    Set<String> accepted = new TreeSet<>();
    Set<String> occurring = new HashSet<>();
    for (Map.Entry<Problem, Set<String>> e : thin.entrySet()) {
      Problem p = e.getKey();
      boolean relevant = p.kind().equals("missing member")
          || p.target().startsWith(RELOCATED_PREFIX.replace('/', '.'))
          || (baselineClasses.contains(p.fromClass()) && !inBaseline.contains(p));
      if (!relevant) {
        continue;
      }
      String key = p.kind().equals("missing member") ? "missing member: " + p.target()
          : "missing classes in " + packageRoot(p.target());
      occurring.add(key);
      if (inBaseline.contains(p) || known.contains(key)) {
        accepted.add(key);
        continue;
      }
      Group g = failing.computeIfAbsent(key, k -> new Group(new TreeSet<>(), new TreeSet<>(), new TreeSet<>()));
      g.targets().add(p.target());
      g.from().add(p.fromClass().replace('/', '.'));
      g.classPaths().addAll(e.getValue());
    }
    if (!accepted.isEmpty()) {
      System.out.println("Known problems, also in the default distribution:");
      accepted.forEach(t -> System.out.println("  " + t));
    }
    // A class that is missing only when a plugin is selected alone is an optional dependency that
    // pinot-all used to supply: a warning. A missing member, or a missing relocated class, always fails.
    int[] failures = {0};
    failing.forEach((key, g) -> {
      boolean selectionOnly = !g.classPaths().contains("all plugins");
      boolean warning = selectionOnly && key.startsWith("missing classes") && !key.contains(".shaded.");
      failures[0] += warning ? 0 : 1;
      System.out.println((warning ? "  WARN " : "  FAIL ") + key
          + (g.targets().size() > 1 ? " (" + g.targets().size() + " classes)" : "")
          + (selectionOnly ? "  [not on the class path with all plugins]" : ""));
      g.from().stream().limit(3).forEach(c -> System.out.println("         from " + c));
      if (g.from().size() > 3) {
        System.out.println("         ... and " + (g.from().size() - 3) + " more classes");
      }
      System.out.println("         on " + String.join(", ", g.classPaths()));
    });
    for (String stale : new TreeSet<>(known)) {
      if (!occurring.contains(stale)) {
        System.out.println("  FAIL known problem no longer occurs, remove it from the known file: " + stale);
        failures[0]++;
      }
    }
    System.out.println("new linkage problems: " + failing.size() + ", failing: " + failures[0]);
    return failures[0] == 0 ? 0 : 1;
  }

  /// The package that groups missing classes: the first three segments, or the relocated library.
  static String packageRoot(String className) {
    String[] parts = className.split("\\.");
    int n = className.startsWith(RELOCATED_PREFIX.replace('/', '.')) ? 7 : 3;
    return String.join(".", List.of(parts).subList(0, Math.min(n, parts.length - 1))) + ".*";
  }

  /// The class paths that the launchers build, by name, in class path order.
  Map<String, List<Path>> classPaths(Path dist)
      throws IOException {
    List<Path> core = jars(dist.resolve("lib"), false);
    Map<String, List<Path>> result = new LinkedHashMap<>();
    List<Path> all = new ArrayList<>(core);
    for (Path pluginDir : pluginDirs(dist.resolve("plugins"))) {
      List<Path> cp = new ArrayList<>(core);
      cp.addAll(jars(pluginDir, false));
      all.addAll(jars(pluginDir, false));
      Path index = pluginDir.resolve(INDEX);
      if (Files.exists(index)) {
        for (String entry : Files.readString(index).split("[:\\n]")) {
          Path jar = dist.resolve(entry.strip());
          if (!entry.isBlank() && Files.isRegularFile(jar) && !cp.contains(jar)) {
            cp.add(jar);
          }
        }
      }
      result.put("PLUGINS_INCLUDE=" + pluginDir.getFileName(), cp);
    }
    all.addAll(jars(dist.resolve("plugin-libs"), false));
    result.put("all plugins", all);
    // LaunchSparkDataIngestionJob puts the plugin jars before pinot-all.
    for (Path pluginDir : pluginDirs(dist.resolve("plugins-external"))) {
      List<Path> cp = new ArrayList<>(jars(pluginDir, false));
      cp.addAll(core);
      result.put("external " + pluginDir.getFileName(), cp);
    }
    return result;
  }

  /// Linkage problems by class path. `loaded` receives the name of every class that some class path
  /// loads, so that a missing class can be compared only for classes that both distributions load.
  Map<Problem, Set<String>> linkageProblems(Path dist, Set<String> loaded)
      throws IOException {
    Map<Problem, Set<String>> problems = new HashMap<>();
    for (Map.Entry<String, List<Path>> cpEntry : classPaths(dist).entrySet()) {
      Map<String, ClassInfo> effective = new HashMap<>();
      for (Path jar : cpEntry.getValue()) {
        load(jar).forEach(effective::putIfAbsent);
      }
      Set<String> checkedJars = new HashSet<>();
      for (Path jar : cpEntry.getValue()) {
        // The core jar resolves against itself first, and its problems repeat on every class path:
        // check it only on the class path that loads all plugins.
        if (!checkedJars.add(jar.toString()) || (jar.getParent().endsWith("lib") && !cpEntry.getKey()
            .equals("all plugins"))) {
          continue;
        }
        for (Map.Entry<String, ClassInfo> c : load(jar).entrySet()) {
          if (effective.get(c.getKey()) != c.getValue() || jdk(c.getKey()) != null) {
            continue; // shadowed by the JDK or an earlier jar, never loaded
          }
          loaded.add(c.getKey());
          checkClass(c.getKey(), c.getValue(), effective, cpEntry.getKey(), problems);
        }
      }
    }
    return problems;
  }

  void checkClass(String name, ClassInfo info, Map<String, ClassInfo> effective, String cpName,
      Map<Problem, Set<String>> problems) {
    for (String classRef : info.classRefs()) {
      if (!effective.containsKey(classRef) && jdk(classRef) == null) {
        problems.computeIfAbsent(new Problem("missing class", name, classRef.replace('/', '.')),
            k -> new TreeSet<>()).add(cpName);
      }
    }
    for (Ref ref : info.refs()) {
      ClassInfo owner = jdk(ref.owner());
      owner = owner != null ? owner : effective.get(ref.owner());
      // Same jar: the jar is consistent with itself. Missing owner: reported as a missing class.
      if (owner == null || owner.jar().equals(info.jar())) {
        continue;
      }
      Boolean found = resolve(ref, ref.owner(), effective, new HashMap<>());
      if (Boolean.FALSE.equals(found)) {
        problems.computeIfAbsent(new Problem("missing member", name, ref.render()), k -> new TreeSet<>())
            .add(cpName);
      }
    }
  }

  /// Resolves a member the way the JVM does: the class, then its superinterfaces and superclasses.
  /// Returns null when the hierarchy reaches a class that is not available, so the answer is unknown.
  Boolean resolve(Ref ref, String className, Map<String, ClassInfo> effective, Map<String, Boolean> visited) {
    // Each class is resolved once per reference: an interface reached twice gives the first answer,
    // and a cycle (which a valid hierarchy does not have) ends as not found.
    if (visited.containsKey(className)) {
      return visited.get(className);
    }
    visited.put(className, false);
    Boolean result = resolveUncached(ref, className, effective, visited);
    visited.put(className, result);
    return result;
  }

  Boolean resolveUncached(Ref ref, String className, Map<String, ClassInfo> effective,
      Map<String, Boolean> visited) {
    ClassInfo c = jdk(className);
    if (c == null) {
      c = effective.get(className);
    }
    if (c == null) {
      return null;
    }
    if ((ref.method() ? c.methods() : c.fields()).contains(ref.name() + ref.type())) {
      return true;
    }
    if (ref.method() && (className.equals("java/lang/invoke/MethodHandle")
        || className.equals("java/lang/invoke/VarHandle"))) {
      return true; // signature polymorphic
    }
    boolean unknown = false;
    List<String> parents = new ArrayList<>(c.interfaces());
    if (c.superName() != null) {
      parents.add(c.superName());
    }
    for (String parent : parents) {
      Boolean r = resolve(ref, parent, effective, visited);
      if (Boolean.TRUE.equals(r)) {
        return true;
      }
      unknown |= r == null;
    }
    return unknown ? null : false;
  }

  // ---------------------------------------------------------------------------------------------
  // Class file reading
  // ---------------------------------------------------------------------------------------------

  static List<Path> jars(Path dir, boolean recursive)
      throws IOException {
    if (!Files.isDirectory(dir)) {
      return List.of();
    }
    try (Stream<Path> s = recursive ? Files.walk(dir) : Files.list(dir)) {
      return s.filter(p -> p.toString().endsWith(".jar")).sorted().toList();
    }
  }

  Map<String, ClassInfo> load(Path jar)
      throws IOException {
    Map<String, ClassInfo> cached = _jarCache.get(jar);
    if (cached != null) {
      return cached;
    }
    Map<String, ClassInfo> classes = new HashMap<>();
    String jarName = jar.toString();
    try (ZipFile zip = new ZipFile(jar.toFile())) {
      for (ZipEntry e : zip.stream().map(ZipEntry.class::cast).toList()) {
        String n = e.getName();
        // A directory on the class path or a jar without Multi-Release uses the base entries only.
        if (!n.endsWith(".class") || n.startsWith("META-INF/") || n.endsWith("module-info.class")) {
          continue;
        }
        try (InputStream in = zip.getInputStream(e)) {
          ClassInfo info = parse(jarName, in.readAllBytes());
          if (info != null) {
            classes.putIfAbsent(n.substring(0, n.length() - ".class".length()), info);
          }
        }
      }
    }
    _jarCache.put(jar, classes);
    return classes;
  }

  static ClassInfo parse(String jar, byte[] bytes) {
    ClassModel m;
    try {
      m = ClassFile.of().parse(bytes);
    } catch (IllegalArgumentException e) {
      return null;
    }
    // A constant pool can hold class entries that no code uses, for example left behind when a library
    // relocated its own copy of another one. The JVM never resolves them. Rewrite the class with a new
    // pool, which keeps only the entries in use, as maven-shade-plugin does for the classes it
    // relocates. Without this, such an entry is a problem in the thin layout and not in the default one.
    try {
      m = ClassFile.of().parse(ClassFile.of(ClassFile.ConstantPoolSharingOption.NEW_POOL,
          ClassFile.StackMapsOption.DROP_STACK_MAPS).transformClass(m, ClassTransform.ACCEPT_ALL));
    } catch (IllegalArgumentException | IllegalStateException e) {
      // Keep the original pool.
    }
    Set<String> fields = new HashSet<>();
    m.fields().forEach(f -> fields.add(f.fieldName().stringValue() + f.fieldType().stringValue()));
    Set<String> methods = new HashSet<>();
    m.methods().forEach(x -> methods.add(x.methodName().stringValue() + x.methodType().stringValue()));
    List<Ref> refs = new ArrayList<>();
    Set<String> classRefs = new HashSet<>();
    String self = m.thisClass().asInternalName();
    for (PoolEntry pe : m.constantPool()) {
      if (pe instanceof MemberRefEntry r) {
        String owner = r.owner().asInternalName();
        if (!owner.startsWith("[") && !isJdk(owner) && !owner.equals(self)) {
          refs.add(new Ref(owner.intern(), r.name().stringValue().intern(), r.type().stringValue().intern(),
              !(pe instanceof FieldRefEntry)));
        }
      } else if (pe instanceof ClassEntry ce) {
        String n = ce.asInternalName();
        while (n.startsWith("[")) {
          n = n.substring(1);
        }
        if (n.startsWith("L") && n.endsWith(";")) {
          n = n.substring(1, n.length() - 1);
        }
        if (n.length() > 1 && !isJdk(n) && !n.equals(self)) {
          classRefs.add(n.intern());
        }
      }
    }
    return new ClassInfo(jar, m.superclass().map(ClassEntry::asInternalName).orElse(null),
        m.interfaces().stream().map(ClassEntry::asInternalName).toList(), fields, methods, refs, classRefs);
  }

  /// Packages that only the JDK defines. References to them are not recorded.
  static boolean isJdk(String internalName) {
    return internalName.startsWith("java/") || internalName.startsWith("jdk/") || internalName.startsWith("sun/");
  }

  /// The JDK class with this name, or null. The boot and platform class loaders define these classes
  /// before the class path is searched, so a JDK class always wins over a jar that also contains it
  /// (for example `org.w3c.dom` in `xml-apis`).
  ClassInfo jdk(String internalName) {
    return _jdkCache.computeIfAbsent(internalName, n -> {
      try (InputStream in = ClassLoader.getPlatformClassLoader().getResourceAsStream(n + ".class")) {
        return in == null ? null : parse("jdk", in.readAllBytes());
      } catch (IOException e) {
        return null;
      }
    });
  }
}
