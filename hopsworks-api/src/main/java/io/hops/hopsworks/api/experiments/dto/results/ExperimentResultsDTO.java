package io.hops.hopsworks.api.experiments.dto.results;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResultsDTO {

  public ExperimentResultsDTO() {
    //Needed for JAXB
  }

  private ExperimentResult[] hyperparameters;

  private ExperimentResult[] metrics;

  public ExperimentResult[] getHyperparameters() {
    return hyperparameters;
  }

  public void setHyperparameters(ExperimentResult[] hyperparameters) {
    this.hyperparameters = hyperparameters;
  }

  public ExperimentResult[] getMetrics() {
    return metrics;
  }

  public void setMetrics(ExperimentResult[] metrics) {
    this.metrics = metrics;
  }
}