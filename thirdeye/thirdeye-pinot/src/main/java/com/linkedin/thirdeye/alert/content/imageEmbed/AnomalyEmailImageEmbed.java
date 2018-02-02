package com.linkedin.thirdeye.alert.content.imageEmbed;

import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.EmailScreenshotHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Embed anomaly screenshot to an email instance.
 * Note that, currently we are not able to provide test for this one, as taking screenshot is not able to be trigger
 * during test
 */
public class AnomalyEmailImageEmbed implements EmailImageEmbed{
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyEmailImageEmbed.class);
  public String imagePath;
  public String name;

  public AnomalyEmailImageEmbed(long anomalyId, String name, EmailContentFormatterConfiguration configuration)
      throws JobExecutionException {
    this.name = name;
    imagePath = EmailScreenshotHelper.takeGraphScreenShot(Long.toString(anomalyId), configuration);
  }

  @Override
  public String embed(HtmlEmail email) throws EmailException {
    String cid = "";
    if (org.apache.commons.lang3.StringUtils.isNotBlank(imagePath)) {
      cid = email.embed(new File(imagePath));
    }
    return cid;
  }

  @Override
  public void cleanup() throws IOException {
    if (org.apache.commons.lang3.StringUtils.isNotBlank(imagePath)) {
      Files.deleteIfExists(new File(imagePath).toPath());
      imagePath = null;
    }
  }

  /**
   * get the key of the image cid
   * @return image cid
   */
  @Override
  public String getName() {
    return name;
  }
}
