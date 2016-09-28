package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;

@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class AbstractBean extends AbstractDTO {

}
