package io.hops.hopsworks.common.experiments.dto;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResultsWrapper {
  private ExperimentResultsDTO[] results;

  public ExperimentResultsWrapper() {
    //Needed for JAXB
  }

  public ExperimentResultsDTO[] getResults() {
    return results;
  }

  public void setResults(ExperimentResultsDTO[] results) {
    this.results = results;
  }
}
