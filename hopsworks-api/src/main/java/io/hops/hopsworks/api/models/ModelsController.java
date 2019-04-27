package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.models.dto.ModelDTO;
import io.hops.hopsworks.api.models.dto.ModelSummary;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.ModelsException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.Path;
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
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ModelsController {

  private static final Logger LOGGER = Logger.getLogger(ModelsController.class.getName());

  @EJB
  private DistributedFsService dfs;
  @EJB
  private ProvenanceController provenanceController;

  public void attachModel(Project project, String userFullName, ModelSummary modelSummary,
                      ModelDTO.XAttrSetFlag xAttrSetFlag)
      throws DatasetException {

    modelSummary.setUserFullName(userFullName);

    String modelPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_MODELS_DATASET + "/" +
        modelSummary.getName() + "/" + modelSummary.getVersion();

    DistributedFileSystemOps dfso = null;
    try {
      dfso = dfs.getDfsOps();

      JAXBContext sparkJAXBContext = JAXBContextFactory.createContext(new Class[] {ModelSummary.class},
          null);
      Marshaller marshaller = sparkJAXBContext.createMarshaller();
      marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      StringWriter sw = new StringWriter();
      marshaller.marshal(modelSummary, sw);
      byte[] model = sw.toString().getBytes(StandardCharsets.UTF_8);
      LOGGER.log(Level.SEVERE, "EXPERIMENT: attaching xattr " + xAttrSetFlag.name());

      EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
      flags.add(XAttrSetFlag.valueOf(xAttrSetFlag.name()));

      dfso.setXAttr(modelPath, "provenance.model_summary", model, flags);
    } catch(IOException | JAXBException ex) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.ATTACH_XATTR_ERROR, Level.SEVERE,
          "path: " + modelPath, ex.getMessage(), ex);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
  }

  public void delete(String name, String version,
                     Project project, String hdfsUser) throws DatasetException, ModelsException {
    boolean success = false;
    DistributedFileSystemOps dfso = null;
    String modelPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_MODELS_DATASET + "/" + name +
        "/" + version;
    try {
      dfso = dfs.getDfsOps(hdfsUser);
      Path path = new Path(modelPath);
      if(!dfso.exists(path)) {
        throw new ModelsException(RESTCodes.ModelsErrorCode.MODEL_NOT_FOUND, Level.FINE);
      }
      success = dfso.rm(path, true);
    } catch (IOException ioe) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.INODE_DELETION_ERROR, Level.SEVERE,
          "path: " + modelPath);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
    if (!success) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.INODE_DELETION_ERROR, Level.FINE,
          "path: " + modelPath);
    }
  }
}
