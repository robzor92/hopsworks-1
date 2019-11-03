package io.hops.hopsworks.api.experiments.dto;

import io.hops.hopsworks.api.experiments.dto.provenance.ExperimentProvenanceDTO;
import io.hops.hopsworks.api.experiments.dto.results.ExperimentResultSummaryDTO;
import io.hops.hopsworks.common.api.RestDTO;
import io.hops.hopsworks.common.dao.tensorflow.config.TensorBoardDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
public class ExperimentDTO extends RestDTO<ExperimentDTO> {

  public ExperimentDTO() {
    //Needed for JAXB
  }

  private String id;

  private String started;

  private String finished;

  private String state;

  private String name;

  private String description;

  private Double metric;

  private String userFullName;

  private String function;

  private String experimentType;

  private String direction;

  private String optimizationKey;

  private String model;

  private String jobName;

  private String appId;

  private String bestDir;

  private ExperimentResultSummaryDTO results;

  private TensorBoardDTO tensorboard;

  private ExperimentProvenanceDTO provenance;

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

  public Double getMetric() {
    return metric;
  }

  public void setMetric(Double metric) {
    this.metric = metric;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
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

  public TensorBoardDTO getTensorboard() {
    return tensorboard;
  }

  public void setTensorboard(TensorBoardDTO tensorboard) {
    this.tensorboard = tensorboard;
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

  public ExperimentProvenanceDTO getProvenance() {
    return provenance;
  }

  public void setProvenance(ExperimentProvenanceDTO provenance) {
    this.provenance = provenance;
  }

  public ExperimentResultSummaryDTO getResults() {
    return results;
  }

  public void setResults(ExperimentResultSummaryDTO results) {
    this.results = results;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
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

  public enum XAttrSetFlag {
    CREATE,
    REPLACE;

    public static XAttrSetFlag fromString(String param) {
      return valueOf(param.toUpperCase());
    }
  }
}
