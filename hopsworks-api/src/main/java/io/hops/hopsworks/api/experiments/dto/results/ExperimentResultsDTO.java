package io.hops.hopsworks.api.experiments.dto.results;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResultsDTO {

  public ExperimentResultsDTO() {
    //Needed for JAXB
  }

  private ExperimentResult[] parameters;

  private ExperimentResult[] metrics;

  public ExperimentResult[] getMetrics() {
    return metrics;
  }

  public void setMetrics(ExperimentResult[] metrics) {
    this.metrics = metrics;
  }

  public ExperimentResult[] getParameters() {
    return parameters;
  }

  public void setParameters(ExperimentResult[] parameters) {
    this.parameters = parameters;
  }
}