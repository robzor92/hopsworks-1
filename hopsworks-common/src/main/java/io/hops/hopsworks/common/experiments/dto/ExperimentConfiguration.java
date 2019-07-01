package io.hops.hopsworks.common.experiments.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ExperimentConfiguration {

  @XmlElement
  private String name;

  @XmlElement
  private String description;

  @XmlElement
  private String userFullName;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

}
