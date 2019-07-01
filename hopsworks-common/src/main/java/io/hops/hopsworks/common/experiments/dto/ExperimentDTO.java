package io.hops.hopsworks.common.experiments.dto;

import io.hops.hopsworks.common.api.RestDTO;
import io.hops.hopsworks.common.provenance.Provenance;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class ExperimentDTO extends RestDTO<ExperimentDTO> {

  @XmlElement
  private String type;

  @XmlElement
  private String id;

  @XmlElement
  private String started;

  @XmlElement
  private String finished;

  @XmlElement
  private Provenance.AppState state;

  @XmlElement
  private String name;

  @XmlElement
  private String metric;

  @XmlElement
  private String userFullName;


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

  public Provenance.AppState getState() {
    return state;
  }

  public void setState(Provenance.AppState state) {
    this.state = state;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public enum XAttrSetFlag {
    CREATE,
    REPLACE;

    public static XAttrSetFlag fromString(String param) {
      return valueOf(param.toUpperCase());
    }
  }
}
