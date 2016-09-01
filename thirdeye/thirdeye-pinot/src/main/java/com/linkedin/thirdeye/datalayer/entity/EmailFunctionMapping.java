package com.linkedin.thirdeye.datalayer.entity;

import java.util.List;

public class EmailFunctionMapping
    extends AbstractMappingEntity<EmailConfiguration, AnomalyFunction> {

  public EmailFunctionMapping(List<Long> emailConfigIds, List<Long> anomalyFunctionIds) {
    super(emailConfigIds, anomalyFunctionIds);
  }
}
