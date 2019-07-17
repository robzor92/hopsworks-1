package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.experiments.dto.ExperimentResult;
import io.hops.hopsworks.common.experiments.dto.ExperimentResultsDTO;
import io.hops.hopsworks.common.provenance.GeneralQueryParams;
import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.MLAssetHit;
import io.hops.hopsworks.common.provenance.MLAssetListQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.util.DateUtils;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.UriInfo;
import java.util.List;
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
          elasticController.fileProvenanceByMLType(Provenance.MLType.EXPERIMENT.name(),
              queryParams, new GeneralQueryParams(false))) {
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
      if (config.has("name")) {
        experimentDTO.setName(config.get("name").toString());
      }
      if (config.has("userFullName")) {
        experimentDTO.setUserFullName(config.get("userFullName").toString());
      }
      if (config.has("metric")) {
        experimentDTO.setMetric(config.get("metric").toString());
      }
      if (config.has("description")) {
        experimentDTO.setDescription(config.get("description").toString());
      }
      Long creationTime = fileProvenanceHit.getCreateTime();

      experimentDTO.setStarted(DateUtils.millis2LocalDateTime(creationTime).toString());
      Long finishTime = null;
      if (config.has("duration")) {
        finishTime = creationTime + Long.valueOf((String) config.get("duration"));
        experimentDTO.setFinished(DateUtils.millis2LocalDateTime(finishTime).toString());
      }
      if (config.has("state")) {
        Provenance.AppState state = Provenance.AppState.valueOf(config.get("state").toString());
        experimentDTO.setState(state.name());

        if (1 == 3) {
          if (state.equals(Provenance.AppState.RUNNING)) {
            MLAssetAppState appState = fileProvenanceHit.getAppState();
            Provenance.AppState currentState = appState.getCurrentState();
            if (appState != null && currentState != null) {
              experimentDTO.setStarted(fileProvenanceHit.getAppState().getReadableStartTime());
              experimentDTO.setState(currentState.name());
              if (currentState.isFinalState()) {
                experimentDTO.setFinished(fileProvenanceHit.getAppState().getReadableFinishTime());
              }
            }
          } else {
          }
        }
      }
      if (config.has("results")) {
        JSONArray results = config.getJSONArray("results");
        ExperimentResultsDTO[] experimentResults = new ExperimentResultsDTO[results.length()];
        for (int resultIndex = 0; resultIndex < results.length(); resultIndex++) {
          ExperimentResultsDTO resultsDTO = new ExperimentResultsDTO();
          JSONObject experiment = results.getJSONObject(resultIndex);
          JSONArray metrics = experiment.getJSONArray("metrics");
          ExperimentResult[] metricResults = new ExperimentResult[metrics.length()];
          for(int metricIndex = 0; metricIndex < metrics.length(); metricIndex++) {
            ExperimentResult metricResult = new ExperimentResult((JSONObject)metrics.get(metricIndex));
            metricResults[metricIndex] = metricResult;
          }
          resultsDTO.setMetrics(metricResults);

          JSONArray hyperparameters = experiment.getJSONArray("hyperparameters");
          ExperimentResult[] hyperparameterResults = new ExperimentResult[hyperparameters.length()];
          for(int hyperparameterIndex = 0; hyperparameterIndex < hyperparameters.length(); hyperparameterIndex++) {
            ExperimentResult hyperparameterResult =
                new ExperimentResult((JSONObject)hyperparameters.get(hyperparameterIndex));
            hyperparameterResults[hyperparameterIndex] = hyperparameterResult;
          }
          resultsDTO.setHyperparameters(hyperparameterResults);

          experimentResults[resultIndex] = resultsDTO;
        }
        experimentDTO.setResults(experimentResults);
      }
    }

    expand(experimentDTO, resourceRequest);
    if (experimentDTO.isExpand()) {

      //dto.setId(job.getId());
    }
    return experimentDTO;
  }
}
