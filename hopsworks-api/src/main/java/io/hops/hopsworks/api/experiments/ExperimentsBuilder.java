package io.hops.hopsworks.api.experiments;

import com.google.common.base.Strings;
import io.hops.hopsworks.api.experiments.provenance.ExperimentFileProvenanceBuilder;
import io.hops.hopsworks.api.experiments.results.ExperimentResultsBuilder;
import io.hops.hopsworks.api.experiments.tensorboard.TensorBoardBuilder;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.AbstractFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.experiments.ExperimentConfigurationConverter;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.experiments.dto.ExperimentDescription;

import io.hops.hopsworks.common.provenance.ProvFileStateHit;
import io.hops.hopsworks.common.provenance.ProvFileStateListParamBuilder;
import io.hops.hopsworks.common.provenance.Provenance;

import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.util.DateUtils;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.UriInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsBuilder {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsBuilder.class.getName());

  @EJB
  private ProvenanceController provenanceController;
  @EJB
  private TensorBoardBuilder tensorBoardBuilder;
  @EJB
  private ExperimentFileProvenanceBuilder experimentFileProvenanceBuilder;
  @EJB
  private ExperimentResultsBuilder experimentResultsBuilder;
  @EJB
  private ExperimentConfigurationConverter experimentConfigurationConverter;

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project, ProvFileStateHit fileProvenanceHit) {
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
      throws ServiceException, ProjectException, GenericException, ExperimentsException {
    ExperimentDTO dto = new ExperimentDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);

    if(dto.isExpand()) {
      ProvFileStateListParamBuilder provFilesParamBuilder = new ProvFileStateListParamBuilder();
      provFilesParamBuilder.withProjectId(project.getId());
      provFilesParamBuilder.withMlType(Provenance.MLType.EXPERIMENT.name());
      provFilesParamBuilder.withAppState(true);
      buildFilter(provFilesParamBuilder, resourceRequest.getFilter());

      GenericEntity<List<ProvFileStateHit>> searchResults = new GenericEntity<List<ProvFileStateHit>>(
          provenanceController.provFileState(provFilesParamBuilder.fileDetails(),
              provFilesParamBuilder.mlAssetDetails(), provFilesParamBuilder.appDetails())) {
      };

      for(ProvFileStateHit fileProvStateHit: searchResults.getEntity()) {
        ExperimentDTO experimentDTO = build(uriInfo, resourceRequest, project, fileProvStateHit);
        if(experimentDTO != null) {
          dto.addItem(experimentDTO);
        }
      }
    }
    return dto;
  }

  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             ProvFileStateHit fileProvenanceHit) throws ExperimentsException {

    ExperimentDTO experimentDTO = new ExperimentDTO();
    uri(experimentDTO, uriInfo, project, fileProvenanceHit);
    expand(experimentDTO, resourceRequest);

    if (experimentDTO.isExpand()) {
      if(fileProvenanceHit.getXattrs().containsKey("config")) {
        JSONObject config = new JSONObject(fileProvenanceHit.getXattrs().get("config"));

        ExperimentDescription experimentDescription =
            experimentConfigurationConverter.unmarshalDescription(config.toString());

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

        if(fileProvenanceHit.getXattrs().containsKey("model")) {
          String model = fileProvenanceHit.getXattrs().get("model");
          experimentDTO.setModel(model);
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
            resourceRequest.get(ResourceRequest.Name.TENSORBOARD), project, fileProvenanceHit.getMlId()));
        experimentDTO.setProvenance(experimentFileProvenanceBuilder.build(uriInfo,
            resourceRequest.get(ResourceRequest.Name.PROVENANCE), project, fileProvenanceHit.getMlId()));
        experimentDTO.setResults(experimentResultsBuilder.build(uriInfo,
            resourceRequest.get(ResourceRequest.Name.RESULTS), project, fileProvenanceHit.getMlId()));
      } else {
        return null;
      }
    }
    return experimentDTO;
  }

  private void buildFilter(ProvFileStateListParamBuilder provFilesParamBuilder,
                                            Set<? extends AbstractFacade.FilterBy> filters) {
    if(filters != null) {
      for (AbstractFacade.FilterBy filterBy : filters) {
        if(filterBy.getParam().compareToIgnoreCase(Filters.NAME.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("config.name", filterBy.getValue());
          provFilesParamBuilder.withXAttrsLike(map);
        }
      }
    }
  }

  private enum Filters {
    NAME
  }
}