package com.linkedin.thirdeye.datalayer.dto;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import com.linkedin.thirdeye.datalayer.pojo.WebappConfigBean;

/**
 * Entity class for webapp configs. name, collection, type conbination should be unique
 */
@Entity
@Table(name = "webapp_config",
    uniqueConstraints = {@UniqueConstraint(columnNames = {"name", "collection", "type"})})
public class WebappConfigDTO extends WebappConfigBean {


}
