package io.hops.hopsworks.common.experiments;

import io.hops.hopsworks.common.experiments.dto.ExperimentConfiguration;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.logging.Level;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsController {

  @EJB
  private DistributedFsService dfs;

  public void publish(String experimentPath, ExperimentConfiguration experimentConfiguration) throws DatasetException {

    DistributedFileSystemOps dfso = null;
    try {
      JAXBContext sparkJAXBContext = JAXBContextFactory.createContext(new Class[] {ExperimentConfiguration.class},
          null);
      Marshaller marshaller = sparkJAXBContext.createMarshaller();
      marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      StringWriter sw = new StringWriter();
      marshaller.marshal(experimentConfiguration, sw);
      byte[] experiment = sw.toString().getBytes(StandardCharsets.UTF_8);
      dfso = dfs.getDfsOps();

      EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
      flags.add(XAttrSetFlag.CREATE);

      dfso.setXAttr(experimentPath, "user.config", experiment, flags);
    } catch(IOException | JAXBException ex) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.ATTACH_XATTR_ERROR, Level.SEVERE,
          "path: " + experimentPath, ex.getMessage(), ex);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
  }
}
