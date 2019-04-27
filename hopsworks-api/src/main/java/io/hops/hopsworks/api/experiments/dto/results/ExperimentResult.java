package io.hops.hopsworks.api.experiments.dto.results;

import org.w3c.dom.Element;

import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ExperimentResult {

  @XmlAnyElement
  private Element node;

  public ExperimentResult() {
    //Needed for JAXB
  }

  public String getKey() {
    return node.getTagName();
  }

  public String getValue() {
    return node.getTextContent();
  }
}
