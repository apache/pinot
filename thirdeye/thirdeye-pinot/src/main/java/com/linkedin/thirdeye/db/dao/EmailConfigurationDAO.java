package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.db.entity.EmailConfiguration;

public class EmailConfigurationDAO extends AbstractJpaDAO<EmailConfiguration> {

  public EmailConfigurationDAO() {
    super(EmailConfiguration.class);
  }

}
