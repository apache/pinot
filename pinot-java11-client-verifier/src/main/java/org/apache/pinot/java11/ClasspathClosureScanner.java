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
package org.apache.pinot.java11;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;


/// Scans every entry of a classpath and reports any class file whose bytecode version is newer than a given Java
/// feature release can load.
///
/// This complements the hard-coded compiler `release` of 11 on Pinot's client and SPI modules. That pin
/// guarantees Pinot's _own_ bytecode is Java 11 clean, but says nothing about the transitive third-party closure those
/// modules drag in. A dependency bump that pulls in a Java 17+ jar would otherwise only be discovered by a user hitting
/// [UnsupportedClassVersionError] at runtime.
///
/// Two categories of entry are deliberately _not_ violations, because a Java 11 JVM never loads them:
///   - `module-info.class` descriptors, which are compiled at the version of the JDK that built the jar and are
///     ignored entirely on the classpath.
///   - Entries under `META-INF/versions/<n>/` that this JVM would not select. A JVM only performs multi-release
///     lookup at all when the jar manifest carries `Multi-Release: true`, so in a jar without that attribute
///     every versioned entry is an inert resource and is skipped outright. In a genuine multi-release jar, only the
///     entries at or below the target release are selected, so entries above it are skipped and the rest are checked.
///     Directory classpath entries never get multi-release treatment, so their versioned entries are skipped too.
///
/// This class is not thread-safe; a [Result] is produced by a single [#scan] call.
final class ClasspathClosureScanner {
  /// Class file major version for Java 11 is 55; each later feature release adds one.
  private static final int CLASS_FILE_MAJOR_VERSION_OFFSET = 44;
  private static final int CLASS_FILE_MAGIC = 0xCAFEBABE;
  private static final String MULTI_RELEASE_PREFIX = "META-INF/versions/";
  private static final String MODULE_INFO_ENTRY = "module-info.class";
  private static final String MANIFEST_ENTRY = "META-INF/MANIFEST.MF";
  /// Cap the reported violations so a wholesale mistake does not produce megabytes of CI log.
  @VisibleForTesting
  static final int MAX_REPORTED_VIOLATIONS = 40;

  private ClasspathClosureScanner() {
  }

  /// A single class file that a JVM at the target feature release could not load.
  static final class Violation {
    private final String _classpathEntry;
    private final String _classFile;
    private final int _majorVersion;

    Violation(String classpathEntry, String classFile, int majorVersion) {
      _classpathEntry = classpathEntry;
      _classFile = classFile;
      _majorVersion = majorVersion;
    }

    @Override
    public String toString() {
      return String.format("%s!/%s (class file major version %d, needs Java %d)", _classpathEntry, _classFile,
          _majorVersion, _majorVersion - CLASS_FILE_MAJOR_VERSION_OFFSET);
    }
  }

  /// Outcome of a scan, including enough counters to tell a real pass from a vacuous one.
  static final class Result {
    private final List<Violation> _reportedViolations = new ArrayList<>();
    private final List<String> _archiveNames = new ArrayList<>();
    private final List<String> _skippedEntries = new ArrayList<>();
    private final Map<Integer, Integer> _majorVersionHistogram = new TreeMap<>();
    private int _totalViolationCount;
    private int _archivesScanned;
    private int _directoriesScanned;
    private int _classFilesInArchives;
    private int _classFilesInDirectories;

    /// The first {@value #MAX_REPORTED_VIOLATIONS} violations, so a wholesale mistake does not produce megabytes of CI
    /// log. [#getTotalViolationCount()] is the authoritative count.
    List<Violation> getReportedViolations() {
      return _reportedViolations;
    }

    int getTotalViolationCount() {
      return _totalViolationCount;
    }

    /// File names of every archive that was opened, for asserting which artifacts were covered.
    List<String> getArchiveNames() {
      return _archiveNames;
    }

    /// Classpath entries that carried no bytecode to check, each with a reason. Callers should treat a non-empty list
    /// as suspicious rather than routine -- see the allow-list in the verifier.
    List<String> getSkippedEntries() {
      return _skippedEntries;
    }

    int getArchivesScanned() {
      return _archivesScanned;
    }

    int getDirectoriesScanned() {
      return _directoriesScanned;
    }

    /// Class files inspected inside jars, counted separately from [#getClassFilesInDirectories()] so a vacuity
    /// guard can assert that third-party bytecode was actually looked at. Counting both together would let the
    /// verifier's own `target/classes` satisfy the guard on its own.
    int getClassFilesInArchives() {
      return _classFilesInArchives;
    }

    int getClassFilesInDirectories() {
      return _classFilesInDirectories;
    }

    /// Major version to class file count, so the CI log shows what the scan actually looked at. A pass whose histogram
    /// is empty, or whose highest bucket is implausibly low, is a broken scan rather than a clean closure.
    Map<Integer, Integer> getMajorVersionHistogram() {
      return _majorVersionHistogram;
    }

    private void record(String classpathEntry, String classFile, int majorVersion, int maxAllowedMajorVersion,
        boolean insideArchive) {
      if (insideArchive) {
        _classFilesInArchives++;
      } else {
        _classFilesInDirectories++;
      }
      _majorVersionHistogram.merge(majorVersion, 1, Integer::sum);
      if (majorVersion > maxAllowedMajorVersion) {
        _totalViolationCount++;
        if (_reportedViolations.size() < MAX_REPORTED_VIOLATIONS) {
          _reportedViolations.add(new Violation(classpathEntry, classFile, majorVersion));
        }
      }
    }
  }

  /// Scans `classpath` (a [File#pathSeparator] -delimited list) for class files that a JVM at
  /// `targetJavaFeatureVersion` could not load.
  ///
  /// @throws IOException if a `.jar`/`.zip` entry on the classpath cannot be opened or read. A corrupt
  /// archive is a hard error, never a silent skip.
  static Result scan(String classpath, int targetJavaFeatureVersion)
      throws IOException {
    int maxAllowedMajorVersion = targetJavaFeatureVersion + CLASS_FILE_MAJOR_VERSION_OFFSET;
    Result result = new Result();
    for (String entry : classpath.split(File.pathSeparator)) {
      if (entry.isEmpty()) {
        continue;
      }
      File file = new File(entry);
      if (!file.exists()) {
        result._skippedEntries.add(entry + " (does not exist)");
        continue;
      }
      if (file.isDirectory()) {
        result._directoriesScanned++;
        scanDirectory(file, file, result, targetJavaFeatureVersion, maxAllowedMajorVersion);
        continue;
      }
      String lowerCaseName = file.getName().toLowerCase(Locale.ROOT);
      if (!lowerCaseName.endsWith(".jar") && !lowerCaseName.endsWith(".zip")) {
        // Maven puts `pom`-type dependencies (e.g. groovy-all) on the classpath as .pom files. Those
        // carry no bytecode, so there is nothing to check -- but report them so the skip is visible
        // and the caller can decide whether the reason is one it tolerates.
        result._skippedEntries.add(entry + " (not an archive)");
        continue;
      }
      result._archivesScanned++;
      result._archiveNames.add(file.getName());
      scanArchive(file, result, targetJavaFeatureVersion, maxAllowedMajorVersion);
    }
    return result;
  }

  private static void scanArchive(File archive, Result result, int targetJavaFeatureVersion,
      int maxAllowedMajorVersion)
      throws IOException {
    try (ZipFile zipFile = new ZipFile(archive)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      boolean multiRelease = isMultiRelease(zipFile);
      while (entries.hasMoreElements()) {
        ZipEntry zipEntry = entries.nextElement();
        String name = zipEntry.getName();
        if (zipEntry.isDirectory() || !isLoadableClassEntry(name, targetJavaFeatureVersion, multiRelease)) {
          continue;
        }
        try (InputStream inputStream = zipFile.getInputStream(zipEntry)) {
          readMajorVersion(inputStream).ifPresent(
              majorVersion -> result.record(archive.getPath(), name, majorVersion, maxAllowedMajorVersion, true));
        }
      }
    } catch (ZipException e) {
      throw new IOException("Failed to open archive on the classpath: " + archive, e);
    }
  }

  private static void scanDirectory(File root, File current, Result result, int targetJavaFeatureVersion,
      int maxAllowedMajorVersion)
      throws IOException {
    File[] children = current.listFiles();
    if (children == null) {
      result._skippedEntries.add(current.getPath() + " (unreadable directory)");
      return;
    }
    for (File child : children) {
      if (child.isDirectory()) {
        scanDirectory(root, child, result, targetJavaFeatureVersion, maxAllowedMajorVersion);
        continue;
      }
      String relativePath = root.toPath().relativize(child.toPath()).toString().replace(File.separatorChar, '/');
      // A directory classpath entry never gets multi-release treatment, whatever a MANIFEST.MF inside
      // it might claim, so any versioned entry under it is inert.
      if (!isLoadableClassEntry(relativePath, targetJavaFeatureVersion, false)) {
        continue;
      }
      try (InputStream inputStream = Files.newInputStream(child.toPath())) {
        readMajorVersion(inputStream).ifPresent(
            majorVersion -> result.record(root.getPath(), relativePath, majorVersion, maxAllowedMajorVersion, false));
      }
    }
  }

  /// Returns whether the archive declares `Multi-Release: true` in its manifest. Only such an archive gets
  /// versioned-entry lookup from a JVM; in any other jar the `META-INF/versions/` tree is inert data.
  private static boolean isMultiRelease(ZipFile zipFile)
      throws IOException {
    ZipEntry manifestEntry = zipFile.getEntry(MANIFEST_ENTRY);
    if (manifestEntry == null) {
      // Some archives spell the manifest with different casing; the JAR spec treats it case-insensitively.
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry candidate = entries.nextElement();
        if (candidate.getName().equalsIgnoreCase(MANIFEST_ENTRY)) {
          manifestEntry = candidate;
          break;
        }
      }
    }
    if (manifestEntry == null) {
      return false;
    }
    try (InputStream inputStream = zipFile.getInputStream(manifestEntry)) {
      String value = new Manifest(inputStream).getMainAttributes().getValue(Attributes.Name.MULTI_RELEASE);
      // The attribute value is defined to be case-insensitive; anything other than "true" means not multi-release.
      return value != null && Boolean.parseBoolean(value.trim());
    } catch (IOException e) {
      // A jar we cannot read the manifest of is a real problem, not something to silently treat as single-release.
      throw new IOException("Failed to read the manifest of archive on the classpath: " + zipFile.getName(), e);
    }
  }

  /// Returns true if `name` is a class file that a JVM at `targetJavaFeatureVersion` would actually load
  /// from the classpath. `multiRelease` says whether the enclosing archive declared
  /// `Multi-Release: true`; when it did not, versioned entries are never selected and so are never checked.
  private static boolean isLoadableClassEntry(String name, int targetJavaFeatureVersion, boolean multiRelease) {
    if (!name.endsWith(".class")) {
      return false;
    }
    String effectiveName = name;
    if (name.startsWith(MULTI_RELEASE_PREFIX)) {
      if (!multiRelease) {
        return false;
      }
      int versionEnd = name.indexOf('/', MULTI_RELEASE_PREFIX.length());
      if (versionEnd < 0) {
        return false;
      }
      int version;
      try {
        version = Integer.parseInt(name.substring(MULTI_RELEASE_PREFIX.length(), versionEnd));
      } catch (NumberFormatException e) {
        // Not a well-formed multi-release directory; treat it as an inert resource.
        return false;
      }
      if (version > targetJavaFeatureVersion) {
        return false;
      }
      effectiveName = name.substring(versionEnd + 1);
    }
    // module-info descriptors are ignored on the classpath, whatever version they were compiled at.
    return !effectiveName.equals(MODULE_INFO_ENTRY) && !effectiveName.endsWith('/' + MODULE_INFO_ENTRY);
  }

  /// Reads the class file major version from the first 8 bytes. Returns empty for anything that is not a class file
  /// despite the `.class` name, which does occur in the wild as packaged test data.
  private static Optional<Integer> readMajorVersion(InputStream inputStream)
      throws IOException {
    DataInputStream dataInputStream = new DataInputStream(inputStream);
    int magic;
    int major;
    try {
      magic = dataInputStream.readInt();
      dataInputStream.readUnsignedShort(); // minor version
      major = dataInputStream.readUnsignedShort();
    } catch (EOFException e) {
      return Optional.empty();
    }
    if (magic != CLASS_FILE_MAGIC) {
      return Optional.empty();
    }
    return Optional.of(major);
  }
}
