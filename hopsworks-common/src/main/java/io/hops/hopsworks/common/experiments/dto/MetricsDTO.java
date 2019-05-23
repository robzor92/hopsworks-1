package io.hops.hopsworks.common.experiments.dto;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class MetricsDTO {

  private String name;
  private String value;

  public MetricsDTO() {
  }

  public MetricsDTO(String name, String value) {
    this.setName(name);
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "MetricsDTO{" + "name=" + getName() + ", value=" + value + '}';
  }
}
