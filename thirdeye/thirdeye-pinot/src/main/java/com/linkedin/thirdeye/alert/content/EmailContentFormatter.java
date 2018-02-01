package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import java.util.Collection;
import java.util.Properties;


/**
 * The module actually put data into emails. The responsibility of this interface is:
 *  - convert anomalies information into instances known by email templates
 *  - properly format alert emails
 *  - acquire auxiliary information from outside
 */
public interface EmailContentFormatter {
  /**
   * Initialize the email content formatter
   * @param properties
   * @param configuration
   */
  void init(Properties properties, EmailContentFormatterConfiguration configuration);

  /**
   * Get the email entity for the list of anomaly results.
   * @param alertConfigDTO
   * @param recipients
   * @param subject
   * @param groupId
   * @param groupName
   * @param anomalies
   * @return
   */
  EmailEntity getEmailEntity(AlertConfigDTO alertConfigDTO, String recipients, String subject,
      Long groupId, String groupName, Collection<AnomalyResult> anomalies, EmailContentFormatterContext context);

  /**
   * Cleanup any temporary data
   */
  void cleanup();
}
