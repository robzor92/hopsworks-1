package io.hops.hopsworks.common.experiments;

import io.hops.hopsworks.common.jobs.spark.SparkJobConfiguration;

import java.util.Date;

public class ExperimentConfiguration {

  private String name;

  private String description;

  private String user;

  private Date start;

  private Date finished;

  private String status;

  private String function;

  private String hp;

  private String metrics;

  private SparkJobConfiguration sparkJobConfiguration;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public Date getStart() {
    return start;
  }

  public void setStart(Date start) {
    this.start = start;
  }

  public Date getFinished() {
    return finished;
  }

  public void setFinished(Date finished) {
    this.finished = finished;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getFunction() {
    return function;
  }

  public void setFunction(String function) {
    this.function = function;
  }

  public String getHp() {
    return hp;
  }

  public void setHp(String hp) {
    this.hp = hp;
  }

  public String getMetrics() {
    return metrics;
  }

  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }

  public SparkJobConfiguration getSparkJobConfiguration() {
    return sparkJobConfiguration;
  }

  public void setSparkJobConfiguration(SparkJobConfiguration sparkJobConfiguration) {
    this.sparkJobConfiguration = sparkJobConfiguration;
  }
}
