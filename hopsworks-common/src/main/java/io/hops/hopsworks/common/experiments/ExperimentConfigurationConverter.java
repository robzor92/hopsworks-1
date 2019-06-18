package io.hops.hopsworks.common.experiments;

import io.hops.hopsworks.common.experiments.dto.ExperimentDescription;
import io.hops.hopsworks.common.experiments.dto.ExperimentResultsWrapper;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;

import javax.annotation.PostConstruct;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;

@Singleton
@TransactionAttribute(TransactionAttributeType.NEVER)
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
public class ExperimentConfigurationConverter {

  private static JAXBContext jaxbExperimentDescriptionContext;
  private static JAXBContext jaxbExperimentResultsWrapperContext;

  @PostConstruct
  public void init() {
    try {
      jaxbExperimentDescriptionContext = JAXBContextFactory.
          createContext(new Class[] {ExperimentDescription.class}, null);
    } catch (JAXBException e) {
      e.printStackTrace();
    }
    try {
      jaxbExperimentResultsWrapperContext = JAXBContextFactory.
          createContext(new Class[] {ExperimentResultsWrapper.class}, null);
    } catch (JAXBException e) {
      e.printStackTrace();
    }
  }

  public ExperimentDescription unmarshalDescription(String jsonConfig) {
    try {
      Unmarshaller unmarshaller = jaxbExperimentDescriptionContext.createUnmarshaller();
      StreamSource json = new StreamSource(new StringReader(jsonConfig));
      unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      return unmarshaller.unmarshal(json, ExperimentDescription.class).getValue();
    } catch(Exception e) {
    }
    return null;
  }

  public ExperimentResultsWrapper unmarshalResults(String jsonConfig) {
    try {
      Unmarshaller unmarshaller = jaxbExperimentResultsWrapperContext.createUnmarshaller();
      StreamSource json = new StreamSource(new StringReader(jsonConfig));
      unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      return unmarshaller.unmarshal(json, ExperimentResultsWrapper.class).getValue();
    } catch(Exception e) {
    }
    return null;
  }
}
