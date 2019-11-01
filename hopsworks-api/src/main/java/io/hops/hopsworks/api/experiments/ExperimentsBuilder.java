package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.api.experiments.dto.ExperimentSummary;
import io.hops.hopsworks.api.experiments.provenance.ExperimentFileProvenanceBuilder;
import io.hops.hopsworks.api.experiments.results.ExperimentResultsBuilder;
import io.hops.hopsworks.api.experiments.tensorboard.TensorBoardBuilder;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.AbstractFacade;
import io.hops.hopsworks.common.dao.hdfsUser.HdfsUsers;
import io.hops.hopsworks.common.dao.hdfsUser.HdfsUsersFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.UserFacade;
import io.hops.hopsworks.common.dao.user.Users;

import io.hops.hopsworks.common.elastic.HopsworksElasticClient;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.provenance.Provenance;

import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.util.DateUtils;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.InvalidQueryException;
import io.hops.hopsworks.exceptions.PythonException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.json.JSONObject;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
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
  private ExperimentSummaryConverter experimentSummaryConverter;
  @EJB
  private UserFacade userFacade;
  @EJB
  private HdfsUsersFacade hdfsUsersFacade;
  @EJB
  private HdfsUsersController hdfsUsersController;
  @EJB
  private ExperimentsController experimentsController;

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.EXPERIMENTS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ExperimentDTO uri(ExperimentDTO dto, UriInfo uriInfo, Project project, FileState fileProvenanceHit) {
    dto.setHref(uriInfo.getBaseUriBuilder()
        .path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
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
      throws GenericException, ExperimentsException, DatasetException {
    ExperimentDTO dto = new ExperimentDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);
    dto.setCount(0l);

    validatePagination(resourceRequest);

    if(dto.isExpand()) {
      try {
        ProvFileStateParamBuilder provFilesParamBuilder = buildExperimentProvenanceParams(project, resourceRequest);
        FileStateDTO.PList fileState = provenanceController.provFileStateList(project, provFilesParamBuilder);
        if (fileState != null) {
          List<FileState> experiments = fileState.getItems();
          dto.setCount(fileState.getCount());
          if (experiments != null && !experiments.isEmpty()) {
            for (FileState fileProvStateHit : experiments) {
              ExperimentDTO experimentDTO = build(uriInfo, resourceRequest, project, fileProvStateHit);
              if (experimentDTO != null) {
                dto.addItem(experimentDTO);
              }
            }
          }
        }
      } catch(ServiceException | PythonException se) {
        LOGGER.log(Level.WARNING, "Could not find elastic mapping for models query", se);
        if(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_NO_MAPPING.getCode().equals(se.getErrorCode().getCode())) {
          return dto;
        }
      }
    }
    return dto;
  }

  private ProvFileStateParamBuilder buildExperimentProvenanceParams(Project project, ResourceRequest resourceRequest)
      throws GenericException {

    ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.EXPERIMENT.name())
        .withPagination(resourceRequest.getOffset(), resourceRequest.getLimit())
        .withAppExpansion();

    buildSortOrder(provFilesParamBuilder, resourceRequest.getSort());
    buildFilter(project, provFilesParamBuilder, resourceRequest.getFilter());
    return provFilesParamBuilder;
  }

  //Build specific
  public ExperimentDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             FileState fileProvenanceHit) throws ExperimentsException, DatasetException,
      GenericException, ServiceException, PythonException {

    ExperimentDTO experimentDTO = new ExperimentDTO();
    uri(experimentDTO, uriInfo, project, fileProvenanceHit);
    expand(experimentDTO, resourceRequest);

    if (experimentDTO.isExpand()) {
      if(fileProvenanceHit.getXattrs() != null && fileProvenanceHit.getXattrs().containsKey("experiment_summary")) {
        JSONObject summary = new JSONObject(fileProvenanceHit.getXattrs().get("experiment_summary"));

        ExperimentSummary experimentSummary =
            experimentSummaryConverter.unmarshalDescription(summary.toString());

        boolean updateNeeded = false;

        // if provenance says it's final state, but exp state is running, update exp state accordingly
        // exp state is not guaranteed to have enough time to report terminal state when being killed for example
        if(experimentSummary.getState().equals(Provenance.AppState.RUNNING.name())
          && Provenance.AppState.valueOf(fileProvenanceHit.getAppState().getCurrentState().name()).isFinalState()) {
          updateNeeded = true;
          experimentSummary.setState(fileProvenanceHit.getAppState().getCurrentState().name());
        }

        experimentDTO.setStarted(DateUtils.millis2LocalDateTime(fileProvenanceHit.getCreateTime()).toString());

        if(!experimentSummary.getState().equals(Provenance.AppState.RUNNING.name()) &&
          experimentSummary.getEndTimestamp() == 0.0f) {
          updateNeeded = true;
          if(experimentSummary.getDuration() > 0) {
            long finishedTime = fileProvenanceHit.getCreateTime() + experimentSummary.getDuration();
            experimentSummary.setEndTimestamp(finishedTime);
          } else {
            experimentSummary.setEndTimestamp(fileProvenanceHit.getAppState().getFinishTime());
          }
        }

        if(experimentSummary.getEndTimestamp() != 0.0f) {
          experimentDTO.setFinished(DateUtils.millis2LocalDateTime(
              Long.valueOf(experimentSummary.getEndTimestamp())).toString());
        }

        if(updateNeeded) {
          experimentsController.attachExperiment(fileProvenanceHit.getMlId(), project,
              experimentSummary.getUserFullName(), experimentSummary, ExperimentDTO.XAttrSetFlag.REPLACE, false);
        }

        if(experimentSummary.getEndTimestamp() != 0.0f) {
          experimentDTO.setFinished(DateUtils.millis2LocalDateTime(
              Long.valueOf(experimentSummary.getEndTimestamp())).toString());
        }

        experimentDTO.setState(experimentSummary.getState());

        if(fileProvenanceHit.getXattrs().containsKey("experiment_model")) {
          String model = fileProvenanceHit.getXattrs().get("experiment_model");
          experimentDTO.setModel(model);
        }

        experimentDTO.setId(fileProvenanceHit.getMlId());
        experimentDTO.setName(experimentSummary.getName());
        experimentDTO.setUserFullName(experimentSummary.getUserFullName());
        experimentDTO.setMetric(experimentSummary.getMetric());
        experimentDTO.setDescription(experimentSummary.getDescription());
        experimentDTO.setExperimentType(experimentSummary.getExperimentType());
        experimentDTO.setFunction(experimentSummary.getFunction());
        experimentDTO.setDirection(experimentSummary.getDirection());
        experimentDTO.setOptimizationKey(experimentSummary.getOptimizationKey());
        experimentDTO.setJobName(experimentSummary.getJobName());
        experimentDTO.setAppId(experimentSummary.getAppId());
        experimentDTO.setBestDir(experimentSummary.getBestDir());
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

  private void buildFilter(Project project, ProvFileStateParamBuilder provFilesParamBuilder,
                                            Set<? extends AbstractFacade.FilterBy> filters)
      throws GenericException {
    if(filters != null) {
      for (AbstractFacade.FilterBy filterBy : filters) {
        if(filterBy.getParam().compareToIgnoreCase(Filters.NAME.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("experiment_summary.name", filterBy.getValue());
          provFilesParamBuilder.withXAttrsLike(map);
        } else if(filterBy.getParam().compareToIgnoreCase(Filters.DATE_START_LT.name()) == 0) {
          provFilesParamBuilder.createdBefore(getDate(filterBy.getField(), filterBy.getValue()).getTime());
        } else if(filterBy.getParam().compareToIgnoreCase(Filters.DATE_START_GT.name()) == 0) {
          provFilesParamBuilder.createdAfter(getDate(filterBy.getField(), filterBy.getValue()).getTime());
        } else if(filterBy.getParam().compareToIgnoreCase(Filters.USER.name()) == 0) {
          String userId = filterBy.getValue();
          Users user = userFacade.find(Integer.parseInt(userId));
          String hdfsUserStr = hdfsUsersController.getHdfsUserName(project, user);
          HdfsUsers hdfsUsers = hdfsUsersFacade.findByName(hdfsUserStr);
          provFilesParamBuilder.withUserId(hdfsUsers.getId().toString());
        } else if(filterBy.getParam().compareToIgnoreCase(Filters.STATE.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("experiment_summary.state", filterBy.getValue());
          provFilesParamBuilder.withXAttrsLike(map);
        } else {
          throw new WebApplicationException("Filter by need to set a valid filter parameter, but found: " +
              filterBy.getParam(), Response.Status.NOT_FOUND);
        }
      }
    }
  }

  private Date getDate(String field, String value) {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    try {
      return formatter.parse(value);
    } catch (ParseException e) {
      throw new InvalidQueryException(
          "Filter value for " + field + " needs to set valid format. Expected:yyyy-mm-dd hh:mm:ss but found: " + value);
    }
  }

  public void buildSortOrder(ProvFileStateParamBuilder provFilesParamBuilder,
                             Set<? extends AbstractFacade.SortBy> sort) {
    if(sort != null) {
      for(AbstractFacade.SortBy sortBy: sort) {
        if(sortBy.getValue().compareToIgnoreCase(SortBy.NAME.name()) == 0) {
          provFilesParamBuilder.sortBy("experiment_summary.name",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        } else if(sortBy.getValue().compareToIgnoreCase(SortBy.METRIC.name()) == 0) {
          provFilesParamBuilder.sortBy("experiment_summary.metric",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        } else if(sortBy.getValue().compareToIgnoreCase(SortBy.USER.name()) == 0) {
          provFilesParamBuilder.sortBy("experiment_summary.userFullName",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        } else if(sortBy.getValue().compareToIgnoreCase(SortBy.START.name()) == 0) {
          provFilesParamBuilder.sortBy("create_timestamp",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        }  else if(sortBy.getValue().compareToIgnoreCase(SortBy.END.name()) == 0) {
          provFilesParamBuilder.sortBy("experiment_summary.endTimestamp",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        }  else if(sortBy.getValue().compareToIgnoreCase(SortBy.STATE.name()) == 0) {
          provFilesParamBuilder.sortBy("experiment_summary.state",
              SortOrder.valueOf(sortBy.getParam().getValue()));
        } else {
          throw new WebApplicationException("Sort by need to set a valid sort parameter, but found: " +
              sortBy.getParam(), Response.Status.NOT_FOUND);
        }
      }
    }
  }

  protected enum SortBy {
    NAME,
    METRIC,
    USER,
    START,
    END,
    STATE;
  }

  protected enum Filters {
    NAME,
    DATE_START_LT,
    DATE_START_GT,
    USER,
    STATE
  }

  private void validatePagination(ResourceRequest resourceRequest) {
    if(resourceRequest.getLimit() == null || resourceRequest.getLimit() <= 0) {
      resourceRequest.setLimit(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
    }

    if(resourceRequest.getOffset() == null || resourceRequest.getOffset() <= 0) {
      resourceRequest.setOffset(0);
    }
  }
}