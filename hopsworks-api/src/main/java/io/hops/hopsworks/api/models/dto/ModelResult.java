package io.hops.hopsworks.api.models.dto;

import org.w3c.dom.Element;

import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ModelResult {

  @XmlAnyElement
  private Element node;

  public ModelResult() {
    //Needed for JAXB
  }

  public String getKey() {
    return node.getTagName();
  }

  public String getValue() {
    return node.getTextContent();
  }
}
