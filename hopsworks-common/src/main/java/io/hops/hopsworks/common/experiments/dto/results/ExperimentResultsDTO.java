package io.hops.hopsworks.common.experiments.dto.results;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResultsDTO {

  public ExperimentResultsDTO() {
    //Needed for JAXB
  }

  private ExperimentResult[] hyperparameters;

  private ExperimentResult[] metrics;

  private String optimizationKey;

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

  public void setOptimizationKey(String optimizationKey) {
    this.optimizationKey = optimizationKey;
  }
}
