package io.hops.hopsworks.common.experiments.dto.results;

import io.hops.hopsworks.common.api.RestDTO;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResultSummaryDTO extends RestDTO<ExperimentResultSummaryDTO> {
  private ExperimentResultsDTO[] results;

  public ExperimentResultSummaryDTO() {
    //Needed for JAXB
  }

  public ExperimentResultsDTO[] getResults() {
    return results;
  }

  public void setResults(ExperimentResultsDTO[] results) {
    this.results = results;
  }
}
