package io.hops.hopsworks.api.experiments.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Summary about an experiment
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ExperimentSummary {

  private String name;

  private Double metric;

  private String description;

  private String userFullName;

  private Integer duration;

  private Long endTimestamp;

  private String state;

  private String function;

  private String experimentType;

  private String direction;

  private String optimizationKey;

  private String jobName;

  private String appId;

  private String bestDir;

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

  public Double getMetric() {
    return metric;
  }

  public void setMetric(Double metric) {
    this.metric = metric;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getFunction() {
    return function;
  }

  public void setFunction(String function) {
    this.function = function;
  }

  public String getExperimentType() {
    return experimentType;
  }

  public void setExperimentType(String experimentType) {
    this.experimentType = experimentType;
  }

  public String getDirection() {
    return direction;
  }

  public void setDirection(String direction) {
    this.direction = direction;
  }

  public String getOptimizationKey() {
    return optimizationKey;
  }

  public void setOptimizationKey(String optimizationKey) {
    this.optimizationKey = optimizationKey;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getBestDir() {
    return bestDir;
  }

  public void setBestDir(String bestDir) {
    this.bestDir = bestDir;
  }

  public Integer getDuration() {
    return duration;
  }

  public void setDuration(Integer duration) {
    this.duration = duration;
  }

  public Long getEndTimestamp() {
    return endTimestamp;
  }

  public void setEndTimestamp(Long endTimestamp) {
    this.endTimestamp = endTimestamp;
  }
}
