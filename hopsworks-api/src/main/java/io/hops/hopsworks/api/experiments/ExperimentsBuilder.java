package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.provenance.MLAssetHit;
import io.hops.hopsworks.common.provenance.MLAssetListQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsBuilder {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsResource.class.getName());

  @EJB
  private ElasticController elasticController;

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
          elasticController.fileProvenanceByMLType(Provenance.MLType.EXPERIMENT.name(), queryParams)) {
      };

      searchResults.getEntity().forEach((fileProvenanceHit) ->
          dto.addItem(build(uriInfo, resourceRequest, project, fileProvenanceHit)));
    }

    return dto;
  }


  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             MLAssetHit fileProvenanceHit) {

    ExperimentDTO experimentDTO = new ExperimentDTO();

    uri(experimentDTO, uriInfo, project, fileProvenanceHit);

    experimentDTO.setType(fileProvenanceHit.getMlType());
    experimentDTO.setId(fileProvenanceHit.getMlId());
    experimentDTO.setStarted("");

    if(fileProvenanceHit.getXattrs().containsKey("config")) {
      JSONObject config = new JSONObject(fileProvenanceHit.getXattrs().get("config"));
      if(config.has("name")) {
        experimentDTO.setName(config.get("name").toString());
      }
      if(config.has("userFullName")) {
        experimentDTO.setUserFullName(config.get("userFullName").toString());
      }
      LOGGER.log(Level.SEVERE, "config");
      LOGGER.log(Level.SEVERE, Arrays.toString(config.keySet().toArray()));
      if(config.has("metric")) {
        //LOGGER.log(Level.SEVERE, "before metric");
        experimentDTO.setMetric(config.get("metric").toString());
      }
    }

    Provenance.AppState currentState = fileProvenanceHit.getAppState().getCurrentState();
    Long startTimestamp = fileProvenanceHit.getAppState().getStartTime();
    Long endTimestamp = fileProvenanceHit.getAppState().getFinishTime();

    experimentDTO.setStarted(fileProvenanceHit.getAppState().getReadableSubmitTime());
    experimentDTO.setState(currentState);

    if(currentState.isFinalState()) {
      experimentDTO.setFinished(fileProvenanceHit.getAppState().getReadableFinishTime());
    }

    //LOGGER.log(Level.SEVERE, "xattrs " + fileProvenanceHit.getXattrs().size());
    for(Map.Entry<String, String> kv: fileProvenanceHit.getXattrs().entrySet()) {
      //LOGGER.log(Level.SEVERE,kv.getKey() + "   "  + kv.getValue());
    }

    //LOGGER.log(Level.SEVERE, "map? " + fileProvenanceHit.getMap().size());
    for(Map.Entry<String, String> kv: fileProvenanceHit.getMap().entrySet()) {
      //LOGGER.log(Level.SEVERE,kv.getKey() + "   "  + kv.getValue());
    }

    expand(experimentDTO, resourceRequest);
    if (experimentDTO.isExpand()) {

      //dto.setId(job.getId());
    }
    return experimentDTO;
  }
}
