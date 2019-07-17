package io.hops.hopsworks.common.experiments.dto;

import io.hops.hopsworks.common.api.RestDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
public class ExperimentDTO extends RestDTO<ExperimentDTO> {

  private String type;

  private String id;

  private String started;

  private String finished;

  private String state;

  private String name;

  private String description;

  private String metric;

  private String userFullName;

  private ExperimentResultsDTO[] results;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getStarted() {
    return started;
  }

  public void setStarted(String started) {
    this.started = started;
  }

  public String getFinished() {
    return finished;
  }

  public void setFinished(String finished) {
    this.finished = finished;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public ExperimentResultsDTO[] getResults() {
    return results;
  }

  public void setResults(ExperimentResultsDTO[] results) {
    this.results = results;
  }

  public enum XAttrSetFlag {
    CREATE,
    REPLACE;

    public static XAttrSetFlag fromString(String param) {
      return valueOf(param.toUpperCase());
    }
  }
}
