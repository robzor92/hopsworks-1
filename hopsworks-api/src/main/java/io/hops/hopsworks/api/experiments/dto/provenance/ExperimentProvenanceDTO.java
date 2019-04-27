package io.hops.hopsworks.api.experiments.dto.provenance;

import io.hops.hopsworks.common.api.RestDTO;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentProvenanceDTO extends RestDTO<ExperimentProvenanceDTO> {

  private String [] filesRead;
  private String [] filesOutput;

  public ExperimentProvenanceDTO() {
    //Needed for JAXB
  }

  public String[] getFilesRead() {
    return filesRead;
  }

  public void setFilesRead(String[] filesRead) {
    this.filesRead = filesRead;
  }

  public String[] getFilesOutput() {
    return filesOutput;
  }

  public void setFilesOutput(String[] filesOutput) {
    this.filesOutput = filesOutput;
  }
}
