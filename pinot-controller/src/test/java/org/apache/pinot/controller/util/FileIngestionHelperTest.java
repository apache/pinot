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
package org.apache.pinot.controller.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.NoClosePinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;


@Test(groups = "stateless")
public class FileIngestionHelperTest {
  private static final File DEST_FILE = new File(FileUtils.getTempDirectory(),
      "pinot-ingest-uri-unused-" + UUID.randomUUID());
  private static final String SENSITIVE_PATH_TOKEN = "controller-secret-7f4c.csv";
  private static final String LOCAL_FILE_SYSTEM_DISABLED_MESSAGE =
      "Local filesystem sources are disabled for /ingestFromURI";

  @DataProvider(name = "localSources")
  public static Object[][] localSources() {
    return new Object[][]{
        {URI.create("file:///private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("file:/private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("file:relative/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("FILE:///private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("file://localhost/private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("/private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("relative/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("./relative/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("../relative/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("//localhost/share/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("C:/private/" + SENSITIVE_PATH_TOKEN)},
        {URI.create("C:relative/" + SENSITIVE_PATH_TOKEN)}
    };
  }

  @Test(dataProvider = "localSources")
  public void testRejectsLocalSourcesByDefaultWithoutExposingPath(URI sourceURI) {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(Map.of(), sourceURI, DEST_FILE, false));

    assertEquals(exception.getMessage(), LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    assertFalse(exception.getMessage().contains(SENSITIVE_PATH_TOKEN));
    assertFalse(DEST_FILE.exists());
  }

  @Test
  public void testCopiesLocalFileUriOnlyWhenCompatibilityOptionIsEnabled()
      throws Exception {
    Path tempDir = Files.createTempDirectory("pinot-ingest-uri-test");
    try {
      Path inputFile = tempDir.resolve("input.csv");
      Path destFile = tempDir.resolve("dest.csv");
      Files.writeString(inputFile, "col\nvalue\n", StandardCharsets.UTF_8);

      FileIngestionHelper.copyURIToLocal(Map.of(), inputFile.toUri(), destFile.toFile(), true);

      assertEquals(Files.readString(destFile, StandardCharsets.UTF_8), "col\nvalue\n");
    } finally {
      FileUtils.deleteQuietly(tempDir.toFile());
    }
  }

  @Test
  public void testRejectsLocalSourceBeforeCreatingWorkingDirectory()
      throws Exception {
    Path tempDir = Files.createTempDirectory("pinot-ingest-uri-validation-test");
    try {
      File ingestionDir = tempDir.resolve("ingestion-dir").toFile();
      FileIngestionHelper helper =
          new FileIngestionHelper(null, null, Map.of(), null, ingestionDir, null, false);

      expectThrows(IllegalArgumentException.class,
          () -> helper.buildSegmentAndPush(new FileIngestionHelper.DataPayload(
              URI.create("file:///private/" + SENSITIVE_PATH_TOKEN))));

      assertFalse(ingestionDir.exists());
    } finally {
      FileUtils.deleteQuietly(tempDir.toFile());
    }
  }

  @Test
  public void testRejectsLocalFileSystemAliasesBeforeRegistration() {
    String[] classNames = {
        LocalPinotFS.class.getName(),
        "org.apache.pinot.filesystem.LocalPinotFS",
        "DEFAULT:" + LocalPinotFS.class.getName()
    };
    for (int i = 0; i < classNames.length; i++) {
      String scheme = "localalias" + i;
      Map<String, String> batchConfigMap = Map.of(BatchConfigProperties.INPUT_FS_CLASS, classNames[i]);

      IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
          () -> FileIngestionHelper.copyURIToLocal(batchConfigMap,
              URI.create(scheme + ":///private/" + SENSITIVE_PATH_TOKEN), DEST_FILE, false));

      assertEquals(exception.getMessage(), LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
      assertFalse(exception.getMessage().contains(classNames[i]));
      assertFalse(PinotFSFactory.isSchemeSupported(scheme));
    }
  }

  @Test
  public void testRejectsLocalFileSystemSubclassBeforeConstructionOrRegistration() {
    TestLocalPinotFS.reset();
    String scheme = "localsubclass";
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, TestLocalPinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap,
            URI.create(scheme + ":///private/" + SENSITIVE_PATH_TOKEN), DEST_FILE, false));

    assertEquals(exception.getMessage(), LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    assertEquals(TestLocalPinotFS._constructorCalls, 0);
    assertEquals(TestLocalPinotFS._initCalls, 0);
    assertEquals(TestLocalPinotFS._copyCalls, 0);
    assertFalse(PinotFSFactory.isSchemeSupported(scheme));
  }

  @Test
  public void testRejectsEndpointProvidedLocalDelegateBeforeInitializationOrRegistration() {
    LocalDelegatePinotFS.reset();
    String scheme = "localdelegate";
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, LocalDelegatePinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap,
            URI.create(scheme + ":///private/" + SENSITIVE_PATH_TOKEN), DEST_FILE, false));

    assertEquals(exception.getMessage(), LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    assertEquals(LocalDelegatePinotFS._constructorCalls, 1);
    assertEquals(LocalDelegatePinotFS._initCalls, 0);
    assertEquals(LocalDelegatePinotFS._copyCalls, 0);
    assertEquals(LocalDelegatePinotFS._wrapperCloseCalls, 1);
    assertEquals(LocalDelegatePinotFS._delegateCloseCalls, 0);
    assertFalse(PinotFSFactory.isSchemeSupported(scheme));
  }

  @Test
  public void testRejectsConfiguredNestedLocalDelegate() {
    String scheme = "configuredlocaldelegate";
    PinotFSFactory.register(scheme, LocalDelegatePinotFS.class.getName(), new PinotConfiguration());
    LocalDelegatePinotFS._copyCalls = 0;

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(Map.of(),
            URI.create(scheme + ":///private/" + SENSITIVE_PATH_TOKEN), DEST_FILE, false));

    assertEquals(exception.getMessage(), LOCAL_FILE_SYSTEM_DISABLED_MESSAGE);
    assertEquals(LocalDelegatePinotFS._copyCalls, 0);
  }

  @Test
  public void testCopiesFromRequestConfiguredRemoteFileSystem()
      throws Exception {
    TestRemotePinotFS.reset();
    String scheme = "requestremote";
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, TestRemotePinotFS.class.getName());
    Path destFile = Files.createTempFile("pinot-ingest-uri-remote", ".txt");
    try {
      FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create(scheme + "://bucket/object"),
          destFile.toFile(), false);

      assertEquals(TestRemotePinotFS._initCalls, 1);
      assertEquals(TestRemotePinotFS._copyCalls, 1);
      assertEquals(TestRemotePinotFS._closeCalls, 1);
      assertEquals(Files.readString(destFile, StandardCharsets.UTF_8), TestRemotePinotFS.REMOTE_CONTENT);
    } finally {
      FileUtils.deleteQuietly(destFile.toFile());
    }
  }

  @Test
  public void testCopiesFromRequestConfiguredRemoteDelegate()
      throws Exception {
    TestRemotePinotFS.reset();
    RemoteDelegatePinotFS.reset();
    String scheme = "requestremotedelegate";
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, RemoteDelegatePinotFS.class.getName());
    Path destFile = Files.createTempFile("pinot-ingest-uri-remote-delegate", ".txt");
    try {
      FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create(scheme + "://bucket/object"),
          destFile.toFile(), false);

      assertEquals(TestRemotePinotFS._initCalls, 1);
      assertEquals(TestRemotePinotFS._copyCalls, 1);
      assertEquals(TestRemotePinotFS._closeCalls, 0);
      assertEquals(RemoteDelegatePinotFS._initCalls, 1);
      assertEquals(RemoteDelegatePinotFS._copyCalls, 1);
      assertEquals(RemoteDelegatePinotFS._closeCalls, 1);
      assertEquals(Files.readString(destFile, StandardCharsets.UTF_8), TestRemotePinotFS.REMOTE_CONTENT);
      assertFalse(PinotFSFactory.isSchemeSupported(scheme));
    } finally {
      FileUtils.deleteQuietly(destFile.toFile());
    }
  }

  @Test
  public void testCopiesFromControllerConfiguredRemoteFileSystem()
      throws Exception {
    TestRemotePinotFS.reset();
    String scheme = "configuredremote";
    PinotFSFactory.register(scheme, TestRemotePinotFS.class.getName(), new PinotConfiguration());
    Path destFile = Files.createTempFile("pinot-ingest-uri-configured-remote", ".txt");
    try {
      FileIngestionHelper.copyURIToLocal(Map.of(), URI.create(scheme + "://bucket/object"), destFile.toFile(),
          false);

      assertEquals(TestRemotePinotFS._initCalls, 1);
      assertEquals(TestRemotePinotFS._copyCalls, 1);
      assertEquals(TestRemotePinotFS._closeCalls, 0);
      assertEquals(Files.readString(destFile, StandardCharsets.UTF_8), TestRemotePinotFS.REMOTE_CONTENT);
    } finally {
      FileUtils.deleteQuietly(destFile.toFile());
    }
  }

  @Test
  public void testClosesRequestScopedFileSystemWhenInitializationFails() {
    FailingInitRemotePinotFS.reset();
    String scheme = "failinginitremote";
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, FailingInitRemotePinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create(scheme + "://bucket/object"),
            DEST_FILE, false));

    assertEquals(exception.getMessage(), "Invalid filesystem source for /ingestFromURI");
    assertEquals(FailingInitRemotePinotFS._closeCalls, 1);
    assertFalse(PinotFSFactory.isSchemeSupported(scheme));
  }

  @Test
  public void testSanitizesLinkageErrorDuringInitialization() {
    LinkageErrorInitRemotePinotFS.reset();
    Map<String, String> batchConfigMap =
        Map.of(BatchConfigProperties.INPUT_FS_CLASS, LinkageErrorInitRemotePinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create("linkageerror://bucket/object"),
            DEST_FILE, false));

    assertEquals(exception.getMessage(), "Invalid filesystem source for /ingestFromURI");
    assertFalse(exception.getMessage().contains(SENSITIVE_PATH_TOKEN));
    assertEquals(LinkageErrorInitRemotePinotFS._closeCalls, 1);
  }

  @Test
  public void testRejectsInvalidFileSystemClassWithoutExposingClassName() {
    String className = "org.apache.pinot.spi.filesystem.SensitiveMissingPinotFS";
    Map<String, String> batchConfigMap = Map.of(BatchConfigProperties.INPUT_FS_CLASS, className);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create("invalidremote://bucket/object"),
            DEST_FILE, false));

    assertEquals(exception.getMessage(), "Invalid filesystem source for /ingestFromURI");
    assertFalse(exception.getMessage().contains(className));
  }

  public static class TestLocalPinotFS extends LocalPinotFS {
    private static int _constructorCalls;
    private static int _initCalls;
    private static int _copyCalls;

    public TestLocalPinotFS() {
      _constructorCalls++;
    }

    @Override
    public void init(PinotConfiguration config) {
      _initCalls++;
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      _copyCalls++;
      super.copyToLocalFile(srcUri, dstFile);
    }

    private static void reset() {
      _constructorCalls = 0;
      _initCalls = 0;
      _copyCalls = 0;
    }
  }

  public static class LocalDelegatePinotFS extends NoClosePinotFS {
    private static int _constructorCalls;
    private static int _initCalls;
    private static int _copyCalls;
    private static int _wrapperCloseCalls;
    private static int _delegateCloseCalls;

    public LocalDelegatePinotFS() {
      super(new LocalPinotFS() {
        @Override
        public void close() {
          _delegateCloseCalls++;
        }
      });
      _constructorCalls++;
    }

    @Override
    public void init(PinotConfiguration config) {
      _initCalls++;
      super.init(config);
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      _copyCalls++;
      super.copyToLocalFile(srcUri, dstFile);
    }

    @Override
    public void close() {
      _wrapperCloseCalls++;
    }

    private static void reset() {
      _constructorCalls = 0;
      _initCalls = 0;
      _copyCalls = 0;
      _wrapperCloseCalls = 0;
      _delegateCloseCalls = 0;
    }
  }

  public static class RemoteDelegatePinotFS extends NoClosePinotFS {
    private static int _initCalls;
    private static int _copyCalls;
    private static int _closeCalls;

    public RemoteDelegatePinotFS() {
      super(new TestRemotePinotFS());
    }

    @Override
    public void init(PinotConfiguration config) {
      _initCalls++;
      super.init(config);
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws Exception {
      _copyCalls++;
      super.copyToLocalFile(srcUri, dstFile);
    }

    @Override
    public void close() {
      _closeCalls++;
    }

    private static void reset() {
      _initCalls = 0;
      _copyCalls = 0;
      _closeCalls = 0;
    }
  }

  public static class TestRemotePinotFS extends BasePinotFS {
    private static final String REMOTE_CONTENT = "remote-content";
    private static int _initCalls;
    private static int _copyCalls;
    private static int _closeCalls;

    @Override
    public void init(PinotConfiguration config) {
      _initCalls++;
    }

    @Override
    public boolean mkdir(URI uri) {
      return true;
    }

    @Override
    public boolean delete(URI segmentUri, boolean forceDelete) {
      return true;
    }

    @Override
    protected boolean doMove(URI srcUri, URI dstUri) {
      return true;
    }

    @Override
    public boolean copyDir(URI srcUri, URI dstUri) {
      return true;
    }

    @Override
    public boolean exists(URI fileUri) {
      return true;
    }

    @Override
    public long length(URI fileUri) {
      return REMOTE_CONTENT.length();
    }

    @Override
    public String[] listFiles(URI fileUri, boolean recursive) {
      return new String[0];
    }

    @Override
    public void copyToLocalFile(URI srcUri, File dstFile)
        throws IOException {
      _copyCalls++;
      Files.writeString(dstFile.toPath(), REMOTE_CONTENT, StandardCharsets.UTF_8);
    }

    @Override
    public void copyFromLocalFile(File srcFile, URI dstUri) {
    }

    @Override
    public boolean isDirectory(URI uri) {
      return false;
    }

    @Override
    public long lastModified(URI uri) {
      return 0L;
    }

    @Override
    public boolean touch(URI uri) {
      return true;
    }

    @Override
    public InputStream open(URI uri) {
      return InputStream.nullInputStream();
    }

    @Override
    public void close() {
      _closeCalls++;
    }

    private static void reset() {
      _initCalls = 0;
      _copyCalls = 0;
      _closeCalls = 0;
    }
  }

  public static class FailingInitRemotePinotFS extends TestRemotePinotFS {
    private static int _closeCalls;

    @Override
    public void init(PinotConfiguration config) {
      throw new IllegalStateException("initialization failed");
    }

    @Override
    public void close() {
      _closeCalls++;
    }

    private static void reset() {
      _closeCalls = 0;
    }
  }

  public static class LinkageErrorInitRemotePinotFS extends TestRemotePinotFS {
    private static int _closeCalls;

    @Override
    public void init(PinotConfiguration config) {
      throw new NoClassDefFoundError(SENSITIVE_PATH_TOKEN);
    }

    @Override
    public void close() {
      _closeCalls++;
    }

    private static void reset() {
      _closeCalls = 0;
    }
  }
}
