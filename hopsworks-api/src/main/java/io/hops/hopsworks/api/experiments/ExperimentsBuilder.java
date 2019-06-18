package io.hops.hopsworks.api.experiments;

import com.google.common.base.Strings;
import io.hops.hopsworks.api.experiments.tensorboard.TensorBoardBuilder;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.experiments.ExperimentConfigurationConverter;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.experiments.dto.ExperimentDescription;
import io.hops.hopsworks.common.experiments.dto.ExperimentResultsWrapper;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.GeneralQueryParams;
import io.hops.hopsworks.common.provenance.MLAssetHit;
import io.hops.hopsworks.common.provenance.MLAssetListQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;

import io.hops.hopsworks.common.util.DateUtils;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsBuilder {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsBuilder.class.getName());

  @EJB
  private ElasticController elasticController;
  @EJB
  private DistributedFsService dfs;
  @EJB
  private ExperimentConfigurationConverter experimentConfigurationConverter;
  @EJB
  private TensorBoardBuilder tensorBoardBuilder;

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project, MLAssetHit fileProvenanceHit) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .path(fileProvenanceHit.getMlId())
        .build());
    return dto;
  }

  public ExperimentDTO expand(ExperimentDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.EXPERIMENTS)) {
      dto.setExpand(true);
    }
    return dto;
  }

  //Build collection
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project)
      throws ServiceException, ProjectException, GenericException {
    ExperimentDTO dto = new ExperimentDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);

    if(dto.isExpand()) {
      MLAssetListQueryParams queryParams = MLAssetListQueryParams.projectMLAssets(project.getId(), true);
      GenericEntity<List<MLAssetHit>> searchResults = new GenericEntity<List<MLAssetHit>>(
          elasticController.fileProvenanceByMLType(Provenance.MLType.EXPERIMENT.name(),
              queryParams, new GeneralQueryParams(false))) {
      };
      searchResults.getEntity().forEach((fileProvenanceHit) -> {
        ExperimentDTO experimentDTO = build(uriInfo, resourceRequest, project, fileProvenanceHit);
        if(experimentDTO != null) {
          dto.addItem(experimentDTO);
        }
      });
    }

    return dto;
  }

  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             MLAssetHit fileProvenanceHit) {

    ExperimentDTO experimentDTO = new ExperimentDTO();
    uri(experimentDTO, uriInfo, project, fileProvenanceHit);
    expand(experimentDTO, resourceRequest);

    if (experimentDTO.isExpand()) {
      if(fileProvenanceHit.getXattrs().containsKey("config")) {
        JSONObject config = new JSONObject(fileProvenanceHit.getXattrs().get("config"));

        ExperimentDescription experimentDescription =
            experimentConfigurationConverter.unmarshalDescription(config.toString());

        DistributedFileSystemOps dfso = null;
        try {
          dfso = dfs.getDfsOps();
          String summaryPath = Utils.getProjectPath(project.getName()) + Settings.HOPS_EXPERIMENTS_DATASET + "/"
              + fileProvenanceHit.getMlId() + "/.summary";
          if(dfso.exists(summaryPath)) {
            String summaryJson = dfso.cat(new Path(summaryPath));
            ExperimentResultsWrapper results = experimentConfigurationConverter.unmarshalResults(summaryJson);
            experimentDTO.setResults(results);
          }
        } catch (IOException e) {
          LOGGER.log(Level.WARNING, "Could not process .summary file " + e.toString());
        } finally {
          if (dfso != null) {
            dfs.closeDfsClient(dfso);
          }
        }

        Long creationTime = fileProvenanceHit.getCreateTime();
        experimentDTO.setStarted(DateUtils.millis2LocalDateTime(creationTime).toString());
        Long finishTime = null;
        if (!Strings.isNullOrEmpty(experimentDescription.getDuration())) {
          finishTime = creationTime + Long.valueOf(experimentDescription.getDuration());
          experimentDTO.setFinished(DateUtils.millis2LocalDateTime(finishTime).toString());
        }

        if(experimentDescription.getState().equals(Provenance.AppState.RUNNING.name())) {
          experimentDTO.setState(fileProvenanceHit.getAppState().getCurrentState().name());
        } else {
          experimentDTO.setState(experimentDescription.getState());
        }

        experimentDTO.setId(fileProvenanceHit.getMlId());
        experimentDTO.setName(experimentDescription.getName());
        experimentDTO.setUserFullName(experimentDescription.getUserFullName());
        experimentDTO.setMetric(experimentDescription.getMetric());
        experimentDTO.setDescription(experimentDescription.getDescription());
        experimentDTO.setExperimentType(experimentDescription.getExperimentType());
        experimentDTO.setFunction(experimentDescription.getFunction());
        experimentDTO.setDirection(experimentDescription.getDirection());
        experimentDTO.setOptimizationKey(experimentDescription.getOptimizationKey());
        experimentDTO.setTensorboard(tensorBoardBuilder.build(uriInfo,
            resourceRequest.get(ResourceRequest.Name.EXECUTIONS), project, fileProvenanceHit.getMlId()));
      } else {
        return null;
      }
    }
    return experimentDTO;
  }
}
