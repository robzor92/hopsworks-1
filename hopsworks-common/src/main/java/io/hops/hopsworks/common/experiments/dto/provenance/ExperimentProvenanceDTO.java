package io.hops.hopsworks.common.experiments.dto.provenance;

import io.hops.hopsworks.common.api.RestDTO;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentProvenanceDTO extends RestDTO<ExperimentProvenanceDTO> {

  private String [] filesRead;
  private String [] filesOutput;
  private String [] filesDeleted;

  public ExperimentProvenanceDTO() {
    //Needed for JAXB
  }

  public String[] getFilesRead() {
    return filesRead;
  }

  public void setFilesRead(String[] filesRead) {
    this.filesRead = filesRead;
  }

  public String[] getFilesDeleted() {
    return filesDeleted;
  }

  public void setFilesDeleted(String[] filesDeleted) {
    this.filesDeleted = filesDeleted;
  }

  public String[] getFilesOutput() {
    return filesOutput;
  }

  public void setFilesOutput(String[] filesOutput) {
    this.filesOutput = filesOutput;
  }
}
