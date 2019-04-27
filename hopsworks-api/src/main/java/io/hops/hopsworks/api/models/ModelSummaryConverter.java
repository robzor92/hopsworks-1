package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.models.dto.ModelSummary;
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
public class ModelSummaryConverter {

  private JAXBContext jaxbExperimentSummaryContext;

  @PostConstruct
  public void init() {
    try {
      jaxbExperimentSummaryContext = JAXBContextFactory.
          createContext(new Class[] {ModelSummary.class}, null);
    } catch (JAXBException e) {
      e.printStackTrace();
    }
  }

  public ModelSummary unmarshalDescription(String jsonConfig) {
    try {
      Unmarshaller unmarshaller = jaxbExperimentSummaryContext.createUnmarshaller();
      StreamSource json = new StreamSource(new StringReader(jsonConfig));
      unmarshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      unmarshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      return unmarshaller.unmarshal(json, ModelSummary.class).getValue();
    } catch(Exception e) {
    }
    return null;
  }
}
