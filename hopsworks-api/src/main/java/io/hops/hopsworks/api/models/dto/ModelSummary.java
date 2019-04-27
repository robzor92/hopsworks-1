package io.hops.hopsworks.api.models.dto;

import io.hops.hopsworks.api.experiments.dto.results.ExperimentResult;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Summary about an experiment
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ModelSummary {

  private String name;

  private String version;

  private String userFullName;

  private String experimentId;

  private ExperimentResult[] parameters;

  private ExperimentResult[] outputs;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public ExperimentResult[] getParameters() {
    return parameters;
  }

  public void setParameters(ExperimentResult[] parameters) {
    this.parameters = parameters;
  }

  public String getExperimentId() {
    return experimentId;
  }

  public void setExperimentId(String experimentId) {
    this.experimentId = experimentId;
  }

  public ExperimentResult[] getOutputs() {
    return outputs;
  }

  public void setOutputs(ExperimentResult[] outputs) {
    this.outputs = outputs;
  }
}
