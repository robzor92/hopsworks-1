package io.hops.hopsworks.api.models.dto;

import io.hops.hopsworks.api.experiments.dto.results.ExperimentResult;
import io.hops.hopsworks.common.api.RestDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
public class ModelDTO extends RestDTO<ModelDTO> {

  public ModelDTO() {
    //Needed for JAXB
  }

  private String name;

  private String version;

  private String userFullName;

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

  public ExperimentResult[] getOutputs() {
    return outputs;
  }

  public void setOutputs(ExperimentResult[] outputs) {
    this.outputs = outputs;
  }

  public enum XAttrSetFlag {
    CREATE,
    REPLACE;

    public static XAttrSetFlag fromString(String param) {
      return valueOf(param.toUpperCase());
    }
  }
}
