package com.linkedin.thirdeye.datalayer.pojo;


import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import com.linkedin.thirdeye.datalayer.dto.AbstractDTO;

@MappedSuperclass
@JsonIgnoreProperties(ignoreUnknown=true)
public abstract class AbstractBean extends AbstractDTO {

}
