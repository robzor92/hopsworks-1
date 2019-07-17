package io.hops.hopsworks.common.experiments.dto;

import io.hops.hopsworks.common.jobs.spark.ExperimentType;
import io.hops.hopsworks.common.provenance.Provenance;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ExperimentDescription {

  private String name;

  private String metric;

  private String description;

  private String userFullName;

  private String duration;

  private Provenance.AppState state;

  private ExperimentType experimentType;

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

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String duration() {
    return duration;
  }

  public void duration(String duration) {
    this.duration = duration;
  }

  public Provenance.AppState getState() {
    return state;
  }

  public void setState(Provenance.AppState state) {
    this.state = state;
  }

  public ExperimentType getExperimentType() {
    return experimentType;
  }

  public void setExperimentType(ExperimentType experimentType) {
    this.experimentType = experimentType;
  }
}
