package io.hops.hopsworks.common.experiments.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ExperimentResultsDTO {

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
