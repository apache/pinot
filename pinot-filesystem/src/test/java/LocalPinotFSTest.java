import com.linkedin.pinot.storage.LocalPinotFS;
import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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
public class LocalPinotFSTest {
  private String _tmpDir;
  private File _originalFile;

  @BeforeClass
  public void setUp() {
    _tmpDir = System.getProperty("java.io.tmpdir") + "/" + LocalPinotFSTest.class.getSimpleName();
    File dir = new File(_tmpDir);
    FileUtils.deleteQuietly(dir);
    dir.mkdir();
    _originalFile = new File(dir, "testFile");
    try {
      _originalFile.createNewFile();
    } catch (Exception e) {

    }
    dir.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    new File(_tmpDir).delete();
  }

  @Test
  public void testFS() {
    LocalPinotFS localPinotFS = new LocalPinotFS();
    try {
      String originalPath = _originalFile.getPath();
      URI originalLocationUri = new URI(originalPath);
      Assert.assertTrue(localPinotFS.exists(new URI(_tmpDir)));
      Assert.assertTrue(localPinotFS.exists(originalLocationUri));
      File file = new File(_tmpDir, "secondTestFile");
      URI finalLocationUri = new URI(file.getPath());
      Assert.assertTrue(!localPinotFS.exists(finalLocationUri));
      localPinotFS.copy(originalLocationUri, finalLocationUri);
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));

      localPinotFS.delete(finalLocationUri);
      Assert.assertTrue(!localPinotFS.exists(finalLocationUri));

      localPinotFS.copyFromLocalFile(originalLocationUri, finalLocationUri);
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));
    } catch (Exception e) {

    }

  }
}
