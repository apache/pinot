package org.apache.pinot.server.starter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TempSegmentCleanupTest {

  private File dataDir;

  @BeforeMethod
  public void createTempDir()
      throws IOException {
    dataDir = Files.createTempDirectory("pinot_data").toFile();
  }

  @AfterMethod
  public void deleteTempDir() {
    try {
      Files.delete(dataDir.toPath());
    } catch (IOException e) {
      return;
    }
  }

  private long getDataDirFileCount() {
    return FileUtils.listFiles(dataDir, null, true).stream().count();
  }

  @Test
  public void worksWithNoDatadir()
      throws IOException {
    long currentTimestamp = System.currentTimeMillis();
    deleteTempDir();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
  }

  @Test
  public void worksWithEmptyDirectory()
      throws IOException {
    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 0);
  }

  @Test
  public void deletesTmpDirectories()
      throws IOException {
    for (int i = 0; i < 5; i++) {
      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 15);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 0);
  }

  @Test
  public void deletesNestedTmpDirectories()
      throws IOException {
    for (int i = 0; i < 5; i++) {
      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "nested_2", "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 15);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 0);
  }

  @Test
  public void doesNotDeleteTmpFiles()
      throws IOException {
    for (int i = 0; i < 5; i++) {
      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "segment-" + i);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "tmp-star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "tmp-columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "tmp-creation.meta").toFile().createNewFile());

      Path nestedSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "segment-" + i * 10);
      Assert.assertTrue(Paths.get(nestedSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(nestedSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 30);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 30);
  }

  @Test
  public void onlyDeletesDirectoriesAfterCutoffTime()
      throws IOException, InterruptedException {
    for (int i = 0; i < 5; i++) {
      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 15);
    Thread.sleep(100);
    long currentTimestamp = System.currentTimeMillis();

    for (int i = 0; i < 5; i++) {
      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "tmp-segment2-" + i);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 30);

    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 15);
  }

  @Test
  public void deletesTmpDirectoriesAndKeepsNonTmpDirectories()
      throws IOException {
    for (int i = 0; i < 5; i++) {
      Path tmpSegmentDir = Paths.get(dataDir.getAbsolutePath(), "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(tmpSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "segment-" + i * 10);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 30);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 15);
  }

  @Test
  public void deletesNestedTmpDirectoriesAndKeepsNonTmpDirectories()
      throws IOException {
    for (int i = 0; i < 5; i++) {
      Path tmpSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "nested_2", "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(tmpSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "nested_2", "segment-" + i * 10);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 30);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 15);
  }

  @Test
  public void deletesMixedNestedTmpDirectoriesAndKeepsNonTmpDirectories()
      throws IOException {
    for (int i = 0; i < 5; i++) {

      Path tmpSegmentDir = Paths.get(dataDir.getAbsolutePath(), "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(tmpSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(tmpSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path nestedOnceTmpSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(nestedOnceTmpSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(nestedOnceTmpSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedOnceTmpSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedOnceTmpSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path nestedTwiceTmpSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "nested_2", "tmp-segment-" + i);
      Assert.assertTrue(Paths.get(nestedTwiceTmpSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(nestedTwiceTmpSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedTwiceTmpSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedTwiceTmpSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path segmentDir = Paths.get(dataDir.getAbsolutePath(), "segment-" + i * 10);
      Assert.assertTrue(Paths.get(segmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(segmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path nestedOnceSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "segment-" + i * 10);
      Assert.assertTrue(Paths.get(nestedOnceSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(nestedOnceSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedOnceSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedOnceSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());

      Path nestedTwiceSegmentDir = Paths.get(dataDir.getAbsolutePath(), "nested_1", "nested_2", "segment-" + i * 10);
      Assert.assertTrue(Paths.get(nestedTwiceSegmentDir.toString(), "v3").toFile().mkdirs());
      Assert.assertTrue(
          Paths.get(nestedTwiceSegmentDir.toString(), "v3", "star_tree_index").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedTwiceSegmentDir.toString(), "v3", "columns.psf").toFile().createNewFile());
      Assert.assertTrue(
          Paths.get(nestedTwiceSegmentDir.toString(), "v3", "creation.meta").toFile().createNewFile());
    }
    Assert.assertEquals(getDataDirFileCount(), 90);

    long currentTimestamp = System.currentTimeMillis();
    BaseServerStarter.deleteTempFilesSinceApplicationStart(currentTimestamp, dataDir);
    Assert.assertEquals(getDataDirFileCount(), 45);
  }
}
