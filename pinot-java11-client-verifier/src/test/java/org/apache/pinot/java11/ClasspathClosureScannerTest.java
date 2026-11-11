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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Guards the fail path of [ClasspathClosureScanner].
///
/// The scanner is the only part of the Java 11 verifier that can go red on a dependency bump, and both of its filters
/// (`isLoadableClassEntry` and `readMajorVersion`) fail _open_ -- an input they do not recognise is treated
/// as "not a violation". A regression in either therefore produces a permanently green CI job rather than a red one,
/// which is worse than having no job at all. These tests pin the behaviour that keeps it honest, and need no Java 11
/// JVM, so they run in the normal unit test job.
public class ClasspathClosureScannerTest {
  private static final int JAVA_11_FEATURE_VERSION = 11;
  private static final int JAVA_11_MAJOR_VERSION = 55;
  private static final int JAVA_17_MAJOR_VERSION = 61;
  private static final int JAVA_21_MAJOR_VERSION = 65;
  private static final String MANIFEST = "META-INF/MANIFEST.MF";

  private Path _tempDir;

  @BeforeClass
  public void setUp()
      throws IOException {
    _tempDir = Files.createTempDirectory("closure-scanner-test");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (_tempDir == null) {
      return;
    }
    try (Stream<Path> paths = Files.walk(_tempDir)) {
      paths.sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.delete(path);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  @Test
  public void testPostTargetBytecodeAtJarRootIsAViolation()
      throws IOException {
    Path jar = writeJar("too-new.jar", Map.of("com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION)));

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 1);
    assertEquals(result.getClassFilesInArchives(), 1);
    assertTrue(result.getReportedViolations().get(0).toString().contains("com/example/Foo.class"),
        "the violation should name the offending entry: " + result.getReportedViolations().get(0));
    assertTrue(result.getReportedViolations().get(0).toString().contains("Java 21"),
        "the violation should translate the major version: " + result.getReportedViolations().get(0));
  }

  @Test
  public void testTargetBytecodeIsNotAViolation()
      throws IOException {
    Path jar = writeJar("just-right.jar", Map.of("com/example/Foo.class", classFile(JAVA_11_MAJOR_VERSION)));

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 1);
    assertEquals(result.getMajorVersionHistogram(), Map.of(JAVA_11_MAJOR_VERSION, 1));
  }

  /// A module descriptor is never loaded from the classpath, whatever version built it.
  @Test
  public void testModuleInfoIsIgnoredAtAnyVersion()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("module-info.class", classFile(JAVA_21_MAJOR_VERSION));
    entries.put("META-INF/versions/9/module-info.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("with-module-info.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 0);
  }

  /// Real multi-release jars on the client closure (jackson-core, jersey-common) ship Java 17 and 21 bytecode under
  /// META-INF/versions. Flagging those would fail the job for classes a Java 11 JVM never loads, so this is the
  /// false-positive guard.
  @Test
  public void testMultiReleaseEntriesAboveTargetAreIgnored()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put(MANIFEST, multiReleaseManifest());
    entries.put("com/example/Foo.class", classFile(JAVA_11_MAJOR_VERSION));
    entries.put("META-INF/versions/17/com/example/Foo.class", classFile(JAVA_17_MAJOR_VERSION));
    entries.put("META-INF/versions/21/com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("multi-release.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 1, "only the baseline entry should have been inspected");
  }

  /// The other half of the multi-release rule: in a real multi-release jar a versioned directory at or below the target
  /// _is_ selected, so bytecode too new for it is a real violation. Without this the scanner could skip every
  /// META-INF/versions entry and still look correct.
  @Test
  public void testMultiReleaseEntriesAtOrBelowTargetAreChecked()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put(MANIFEST, multiReleaseManifest());
    entries.put("META-INF/versions/9/com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("multi-release-9.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 1);
  }

  /// A JVM only performs versioned lookup when the manifest says `Multi-Release: true`. Without it the whole
  /// META-INF/versions tree is inert data, so nothing under it may be reported -- including entries at or below the
  /// target, which would otherwise be a false positive on a shaded jar that merged versioned entries but lost the
  /// attribute.
  @Test
  public void testVersionedEntriesAreIgnoredWhenTheManifestDoesNotDeclareMultiRelease()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put(MANIFEST, manifest("Manifest-Version: 1.0\n"));
    entries.put("META-INF/versions/9/com/example/Below.class", classFile(JAVA_21_MAJOR_VERSION));
    entries.put("META-INF/versions/21/com/example/Above.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("not-multi-release.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 0);
  }

  /// Same rule when there is no manifest at all.
  @Test
  public void testVersionedEntriesAreIgnoredWhenThereIsNoManifest()
      throws IOException {
    Path jar = writeJar("no-manifest.jar",
        Map.of("META-INF/versions/9/com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION)));

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 0);
  }

  /// The attribute value is case-insensitive per the JAR spec.
  @Test
  public void testMultiReleaseAttributeValueIsCaseInsensitive()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put(MANIFEST, manifest("Manifest-Version: 1.0\nMulti-Release: TRUE\n"));
    entries.put("META-INF/versions/9/com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("multi-release-caps.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 1);
  }

  @Test
  public void testMalformedMultiReleaseDirectoryIsIgnored()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put(MANIFEST, multiReleaseManifest());
    entries.put("META-INF/versions/notanumber/com/example/Foo.class", classFile(JAVA_21_MAJOR_VERSION));
    entries.put("META-INF/versions/Foo.class", classFile(JAVA_21_MAJOR_VERSION));
    Path jar = writeJar("malformed-multi-release.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 0);
  }

  /// Jars do package non-bytecode files under a .class name; those must not be misread as violations.
  @Test
  public void testEntryWithoutClassFileMagicIsNotCounted()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("com/example/NotReallyAClass.class", "this is not bytecode".getBytes(StandardCharsets.UTF_8));
    entries.put("com/example/Truncated.class", new byte[]{(byte) 0xCA, (byte) 0xFE});
    entries.put("com/example/Real.class", classFile(JAVA_11_MAJOR_VERSION));
    Path jar = writeJar("not-bytecode.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 1, "only the real class file should have been counted");
  }

  @Test
  public void testNonClassEntriesAreIgnored()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    entries.put("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n".getBytes(StandardCharsets.UTF_8));
    entries.put("com/example/resource.json", "{}".getBytes(StandardCharsets.UTF_8));
    Path jar = writeJar("resources-only.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 0);
    assertEquals(result.getClassFilesInArchives(), 0);
    assertEquals(result.getArchivesScanned(), 1);
  }

  /// Directory entries are scanned too, but counted apart so they cannot satisfy the vacuity guard.
  @Test
  public void testDirectoryEntriesAreScannedAndCountedSeparately()
      throws IOException {
    Path classesDir = Files.createDirectories(_tempDir.resolve("classes/com/example"));
    Files.write(classesDir.resolve("Foo.class"), classFile(JAVA_21_MAJOR_VERSION));
    Files.write(classesDir.resolve("module-info.class"), classFile(JAVA_21_MAJOR_VERSION));
    Files.write(classesDir.resolve("notes.txt"), "hello".getBytes(StandardCharsets.UTF_8));

    ClasspathClosureScanner.Result result =
        ClasspathClosureScanner.scan(_tempDir.resolve("classes").toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 1);
    assertEquals(result.getClassFilesInDirectories(), 1);
    assertEquals(result.getClassFilesInArchives(), 0);
    assertEquals(result.getDirectoriesScanned(), 1);
  }

  @Test
  public void testCorruptArchiveIsAHardFailure()
      throws IOException {
    Path notAJar = _tempDir.resolve("corrupt.jar");
    Files.write(notAJar, "definitely not a zip".getBytes(StandardCharsets.UTF_8));

    assertThrows(IOException.class, () -> ClasspathClosureScanner.scan(notAJar.toString(), JAVA_11_FEATURE_VERSION));
  }

  @Test
  public void testNonArchiveAndMissingEntriesAreReportedAsSkipped()
      throws IOException {
    Path pom = _tempDir.resolve("groovy-all-3.0.25.pom");
    Files.write(pom, "<project/>".getBytes(StandardCharsets.UTF_8));
    String missing = _tempDir.resolve("does-not-exist.jar").toString();
    String classpath = pom + File.pathSeparator + missing;

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(classpath, JAVA_11_FEATURE_VERSION);

    assertEquals(result.getSkippedEntries().size(), 2, "both entries should be reported: "
        + result.getSkippedEntries());
    assertTrue(result.getSkippedEntries().get(0).endsWith("(not an archive)"), result.getSkippedEntries().get(0));
    assertTrue(result.getSkippedEntries().get(1).endsWith("(does not exist)"), result.getSkippedEntries().get(1));
    assertEquals(result.getArchivesScanned(), 0);
  }

  @Test
  public void testViolationReportingIsCappedButCountIsNot()
      throws IOException {
    Map<String, byte[]> entries = new LinkedHashMap<>();
    for (int i = 0; i < 100; i++) {
      entries.put("com/example/Foo" + i + ".class", classFile(JAVA_21_MAJOR_VERSION));
    }
    Path jar = writeJar("many-violations.jar", entries);

    ClasspathClosureScanner.Result result = ClasspathClosureScanner.scan(jar.toString(), JAVA_11_FEATURE_VERSION);

    assertEquals(result.getTotalViolationCount(), 100);
    assertEquals(result.getReportedViolations().size(), ClasspathClosureScanner.MAX_REPORTED_VIOLATIONS,
        "reporting should be capped for log sanity while the total count stays exact");
  }

  /// The target version is a parameter, so the same scanner has to work if the floor ever moves.
  @Test
  public void testTargetVersionIsHonoured()
      throws IOException {
    Path jar = writeJar("java17.jar", Map.of("com/example/Foo.class", classFile(JAVA_17_MAJOR_VERSION)));

    assertEquals(ClasspathClosureScanner.scan(jar.toString(), 11).getTotalViolationCount(), 1);
    assertEquals(ClasspathClosureScanner.scan(jar.toString(), 17).getTotalViolationCount(), 0);
    assertEquals(ClasspathClosureScanner.scan(jar.toString(), 21).getTotalViolationCount(), 0);
  }

  @Test
  public void testArchiveNamesAreRecordedForCoverageAssertions()
      throws IOException {
    Path first = writeJar("pinot-spi-1.6.0-SNAPSHOT.jar",
        Map.of("com/example/Foo.class", classFile(JAVA_11_MAJOR_VERSION)));
    Path second = writeJar("pinot-common-1.6.0-SNAPSHOT.jar",
        Map.of("com/example/Bar.class", classFile(JAVA_11_MAJOR_VERSION)));

    ClasspathClosureScanner.Result result =
        ClasspathClosureScanner.scan(first + File.pathSeparator + second, JAVA_11_FEATURE_VERSION);

    assertEquals(result.getArchiveNames(), List.of("pinot-spi-1.6.0-SNAPSHOT.jar", "pinot-common-1.6.0-SNAPSHOT.jar"));
  }

  private Path writeJar(String name, Map<String, byte[]> entries)
      throws IOException {
    Path jar = _tempDir.resolve(name);
    try (OutputStream fileOut = Files.newOutputStream(jar); ZipOutputStream zipOut = new ZipOutputStream(fileOut)) {
      for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
        zipOut.putNextEntry(new ZipEntry(entry.getKey()));
        zipOut.write(entry.getValue());
        zipOut.closeEntry();
      }
    }
    return jar;
  }

  private static byte[] multiReleaseManifest() {
    return manifest("Manifest-Version: 1.0\nMulti-Release: true\n");
  }

  private static byte[] manifest(String content) {
    return content.getBytes(StandardCharsets.UTF_8);
  }

  /// The first 8 bytes of a class file are all the scanner reads: magic, minor version, major version.
  private static byte[] classFile(int majorVersion) {
    return ByteBuffer.allocate(10)
        .putInt(0xCAFEBABE)
        .putShort((short) 0)
        .putShort((short) majorVersion)
        .putShort((short) 0)
        .array();
  }
}
