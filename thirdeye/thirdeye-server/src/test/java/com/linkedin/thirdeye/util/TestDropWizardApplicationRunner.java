package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.ThirdEyeApplication;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;

public class TestDropWizardApplicationRunner
{
  private static final File ROOT_DIR
          = new File(System.getProperty("java.io.tmpdir"), TestDropWizardApplicationRunner.class.getSimpleName());

  private static final int REQUEST_TIMEOUT_MILLIS = 10000;
  @Test
  public void testCreateServer() throws Exception
  {
    ThirdEyeApplication.Config config = new ThirdEyeApplication.Config();
    config.setRootDir(ROOT_DIR.getAbsolutePath());

    DropWizardApplicationRunner.DropWizardServer<ThirdEyeApplication.Config> server
            = DropWizardApplicationRunner.createServer(config, ThirdEyeApplication.class);

    Assert.assertNotNull(server.getMetricRegistry());

    server.start();

    // Try to contact it
    long startTime = System.currentTimeMillis();
    boolean success = false;
    while (System.currentTimeMillis() - startTime < REQUEST_TIMEOUT_MILLIS)
    {
      try
      {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:8080/admin").toURL().openConnection();
        byte[] res = IOUtils.toByteArray(conn.getInputStream());
        if (Arrays.equals(res, "GOOD".getBytes()))
        {
          success = true;
          break;
        }
      }
      catch (Exception e)
      {
        // Re-try
      }
    }
    Assert.assertTrue(success);

    server.stop();
  }
}
