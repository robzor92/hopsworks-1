package io.hops.hopsworks.api.models.dto;

import io.hops.hopsworks.common.api.RestDTO;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Contains configuration and other information about an experiment
 */
@XmlRootElement
public class ModelDTO extends RestDTO<ModelDTO> {

  public ModelDTO() {
    //Needed for JAXB
  }

  private String id;

  private String name;

  private int version;

  private String userFullName;

  private String created;

  private ModelResult metrics;

  private String description;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public String getUserFullName() {
    return userFullName;
  }

  public void setUserFullName(String userFullName) {
    this.userFullName = userFullName;
  }

  public ModelResult getMetrics() {
    return metrics;
  }

  public void setMetrics(ModelResult metrics) {
    this.metrics = metrics;
  }

  public String getCreated() {
    return created;
  }

  public void setCreated(String created) {
    this.created = created;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public enum XAttrSetFlag {
    CREATE,
    REPLACE;

    public static XAttrSetFlag fromString(String param) {
      return valueOf(param.toUpperCase());
    }
  }
}