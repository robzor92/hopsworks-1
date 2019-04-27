package io.hops.hopsworks.api.models.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Summary about a model
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ModelSummary {

  private String name;

  private Integer version;

  private String userFullName;

  private String experimentId;

  private ModelResult metrics;

  private String description;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public void setExperimentId(String experimentId) {
    this.experimentId = experimentId;
  }

  public ModelResult getMetrics() {
    return metrics;
  }

  public void setMetrics(ModelResult metrics) {
    this.metrics = metrics;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
