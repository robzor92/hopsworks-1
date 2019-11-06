package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.api.experiments.dto.ExperimentSummary;
import io.hops.hopsworks.common.dao.jobs.description.Jobs;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.jobs.JobController;
import io.hops.hopsworks.common.jobs.spark.SparkJobConfiguration;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.python.environment.EnvironmentController;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.JobException;
import io.hops.hopsworks.exceptions.PythonException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.parquet.Strings;
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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsController {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsController.class.getName());

  @EJB
  private DistributedFsService dfs;
  @EJB
  private ProvenanceController provenanceController;
  @EJB
  private EnvironmentController environmentController;
  @EJB
  private JobController jobController;
  @EJB
  private DistributedFsService dfsService;
  @EJB
  private HdfsUsersController hdfsUsersController;


  public void attachExperiment(String id, Project project, String usersFullName, ExperimentSummary experimentSummary,
                               ExperimentDTO.XAttrSetFlag xAttrSetFlag)
      throws DatasetException, GenericException, ServiceException {

    experimentSummary.setUserFullName(usersFullName);
    String experimentPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_EXPERIMENTS_DATASET + "/" + id;
    // attempt to set the final timestamp time
    if(xAttrSetFlag.equals(ExperimentDTO.XAttrSetFlag.REPLACE) && experimentSummary.getDuration() != null &&
        experimentSummary.getDuration() > 0) {
      FileState fileState = getExperiment(project, id);
      if(fileState != null && fileState.getCreateTime() != null) {
        Long finishedTime = fileState.getCreateTime() + experimentSummary.getDuration();
        experimentSummary.setEndTimestamp(finishedTime);
      }
    }

    DistributedFileSystemOps dfso = null;
    try {
      dfso = dfs.getDfsOps();
      if(!Strings.isNullOrEmpty(experimentSummary.getAppId()) &&
          xAttrSetFlag.equals(ExperimentDTO.XAttrSetFlag.CREATE)) {
        byte[] appIdBytes = experimentSummary.getAppId().getBytes(StandardCharsets.UTF_8);
        EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
        flags.add(XAttrSetFlag.CREATE);
        dfso.setXAttr(experimentPath, "provenance.app_id", appIdBytes, flags);
      }

      JAXBContext sparkJAXBContext = JAXBContextFactory.createContext(new Class[] {ExperimentSummary.class},
          null);
      Marshaller marshaller = sparkJAXBContext.createMarshaller();
      marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, false);
      marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
      StringWriter sw = new StringWriter();
      marshaller.marshal(experimentSummary, sw);
      byte[] experiment = sw.toString().getBytes(StandardCharsets.UTF_8);

      EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
      flags.add(XAttrSetFlag.valueOf(xAttrSetFlag.name()));

      dfso.setXAttr(experimentPath, "provenance." +
          ExperimentsBuilder.EXPERIMENT_SUMMARY_XATTR_NAME, experiment, flags);

    } catch(IOException | JAXBException ex) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.ATTACH_XATTR_ERROR, Level.SEVERE,
          "path: " + experimentPath, ex.getMessage(), ex);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
  }

  public String[] exportExperimentEnvironment(String id, Project project, Users user)
      throws PythonException, ServiceException {
    return environmentController.exportEnv(user, project,
        Settings.HOPS_EXPERIMENTS_DATASET + "/" + id);
  }

  public void attachModel(String id, Project project, String model,
                               ExperimentDTO.XAttrSetFlag xAttrSetFlag)
      throws DatasetException {
    String experimentPath = Utils.getProjectPath(project.getName()) +
        Settings.HOPS_EXPERIMENTS_DATASET + "/" + id;
    DistributedFileSystemOps dfso = null;
    try {
      byte[] experiment = model.getBytes(StandardCharsets.UTF_8);
      dfso = dfs.getDfsOps();
      EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
      flags.add(XAttrSetFlag.valueOf(xAttrSetFlag.name()));
      dfso.setXAttr(experimentPath, "provenance." +
          ExperimentsBuilder.EXPERIMENT_MODEL_XATTR_NAME, experiment, flags);
    } catch(IOException ex) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.ATTACH_XATTR_ERROR, Level.SEVERE,
          "path: " + experimentPath, ex.getMessage(), ex);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
  }

  public void delete(String id, Project project, String hdfsUser) throws DatasetException {
    boolean success = false;
    DistributedFileSystemOps dfso = null;
    String experimentPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_EXPERIMENTS_DATASET + "/" + id;
    try {
      dfso = dfs.getDfsOps(hdfsUser);
      Path path = new Path(experimentPath);
      if(!dfso.exists(path)) {
        return;
      }
      success = dfso.rm(path, true);
    } catch (IOException ioe) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.INODE_DELETION_ERROR, Level.SEVERE,
          "path: " + experimentPath, "Error occurred during deletion of experiment", ioe);
    } finally {
      if (dfso != null) {
        dfs.closeDfsClient(dfso);
      }
    }
    if (!success) {
      throw new DatasetException(RESTCodes.DatasetErrorCode.INODE_DELETION_ERROR, Level.FINE,
          "path: " + experimentPath);
    }
  }

  public FileState getExperiment(Project project, String mlId) throws GenericException, ServiceException {
    ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.EXPERIMENT.name())
        .withPagination(0, 1)
        .withAppExpansion()
        .filterByHasXAttr(ExperimentsBuilder.EXPERIMENT_SUMMARY_XATTR_NAME)
        .withMlId(mlId);
    FileStateDTO.PList fileState = provenanceController.provFileStateList(project, provFilesParamBuilder);
    if (fileState != null && fileState.getItems() != null) {
      List<FileState> experiments = fileState.getItems();
      if (experiments != null && !experiments.isEmpty()) {
        return experiments.iterator().next();
      }
    }
    return null;
  }

  public void copyExecutable(Project project, Users user, ExperimentSummary experimentSummary, String experimentId)
      throws JobException, ExperimentsException {
    String jobName = experimentSummary.getJobName();
    if(!Strings.isNullOrEmpty(experimentSummary.getJobName())) {
      //job
      Jobs experimentJob = jobController.getJob(project, jobName);
      SparkJobConfiguration sparkJobConf = (SparkJobConfiguration)experimentJob.getJobConfig();
      copy(sparkJobConf.getAppPath(), project, user, experimentId);
    } else {
      //jupyter

    }
  }

  public void copy(String path, Project project, Users user, String experimentId) throws ExperimentsException {
    DistributedFileSystemOps udfso = null;
    try {
      String username = hdfsUsersController.getHdfsUserName(project, user);
      udfso = dfs.getDfsOps(username);
      if(!udfso.exists(path)) {
        throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.EXPERIMENT_EXECUTABLE_NOT_FOUND, Level.FINE,
            "path: " + path);
      } else {
        udfso.copyInHdfs(new Path(path), new Path(Utils.getProjectPath(project.getName()) +
            Settings.HOPS_EXPERIMENTS_DATASET + "/" + experimentId));
      }
    } catch (IOException | ExperimentsException e) {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.EXPERIMENT_EXECUTABLE_COPY_FAILED, Level.FINE,
          "path: " + path);
    } finally {
      if (udfso != null) {
        dfs.closeDfsClient(udfso);
      }
    }
  }
}
