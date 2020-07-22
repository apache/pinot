package org.apache.pinot.spi.crypt;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class NoOpPinotCryptTest {

  PinotCrypter pinotCrypter;
  File srcFile;
  File destinationFile;
  @BeforeTest
  public void init() throws IOException {
    pinotCrypter = new NoOpPinotCrypter();
    srcFile = File.createTempFile("srcFile","txt");
    srcFile.deleteOnExit();
    destinationFile = File.createTempFile("destFile","txt");
    destinationFile.deleteOnExit();
    FileUtils.write(srcFile,"testData");
  }

  @Test
  public void testEncryption() throws IOException {
    pinotCrypter.encrypt(srcFile,destinationFile);
    Assert.assertTrue(FileUtils.contentEquals(srcFile,destinationFile));
  }

  @Test
  public void testDecryption() throws IOException {
    pinotCrypter.decrypt(destinationFile,srcFile);
    Assert.assertTrue(FileUtils.contentEquals(srcFile,destinationFile));
  }

}
