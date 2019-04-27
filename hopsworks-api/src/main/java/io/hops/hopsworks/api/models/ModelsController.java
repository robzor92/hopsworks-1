package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.models.dto.ModelDTO;
import io.hops.hopsworks.api.models.dto.ModelResult;
import io.hops.hopsworks.api.models.dto.ModelSummary;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.XAttrSetFlag;
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;

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

      EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
      flags.add(XAttrSetFlag.valueOf(xAttrSetFlag.name()));

      attachSortableMetadata(modelSummary, dfso, modelPath);

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

  private void attachSortableMetadata(ModelSummary modelSummary, DistributedFileSystemOps dfso, String modelPath)
      throws IOException {

    EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
    flags.add(XAttrSetFlag.CREATE);

    if (modelSummary.getMetrics() != null && modelSummary.getMetrics().length > 0) {
      for (ModelResult metric : modelSummary.getMetrics()) {
        dfso.setXAttr(modelPath, "provenance.model_" + metric.getKey(),
            metric.getValue().getBytes(StandardCharsets.UTF_8), flags);
      }
    }

    if (modelSummary.getParameters() != null && modelSummary.getParameters().length > 0) {
      for (ModelResult parameter : modelSummary.getParameters()) {
        dfso.setXAttr(modelPath, "provenance.model_" + parameter.getKey(),
            parameter.getValue().getBytes(StandardCharsets.UTF_8), flags);
      }
    }
  }

  public FileState getModel(Project project, String mlId) throws GenericException, ServiceException {
    ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.MODEL.name())
        .withPagination(0, 1)
        .withAppExpansion()
        .withMlId(mlId);
    FileStateDTO.PList fileState = provenanceController.provFileStateList(provFilesParamBuilder);
    if (fileState != null) {
      List<FileState> experiments = fileState.getItems();
      if (experiments != null && !experiments.isEmpty()) {
        return experiments.iterator().next();
      }
    }
    return null;
  }
}
