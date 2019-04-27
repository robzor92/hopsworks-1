package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.models.dto.ModelDTO;
import io.hops.hopsworks.api.models.dto.ModelSummary;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.AbstractFacade;
import io.hops.hopsworks.common.dao.hdfsUser.HdfsUsersFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.UserFacade;
import io.hops.hopsworks.common.elastic.HopsworksElasticClient;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.util.DateUtils;
import io.hops.hopsworks.exceptions.GenericException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@Stateless
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ModelsBuilder {

  private static final Logger LOGGER = Logger.getLogger(ModelsBuilder.class.getName());
  @EJB
  private UserFacade userFacade;
  @EJB
  private HdfsUsersFacade hdfsUsersFacade;
  @EJB
  private HdfsUsersController hdfsUsersController;
  @EJB
  private ProvenanceController provenanceController;
  @EJB
  private ModelSummaryConverter modelSummaryConverter;

  public ModelDTO uri(ModelDTO dto, UriInfo uriInfo, Project project) {
    dto.setHref(uriInfo.getBaseUriBuilder().path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.MODELS.toString().toLowerCase())
        .build());
    return dto;
  }

  public ModelDTO uri(ModelDTO dto, UriInfo uriInfo, Project project, FileState fileProvenanceHit) {
    dto.setHref(uriInfo.getBaseUriBuilder()
        .path(ResourceRequest.Name.PROJECT.toString().toLowerCase())
        .path(Integer.toString(project.getId()))
        .path(ResourceRequest.Name.MODELS.toString().toLowerCase())
        .path(fileProvenanceHit.getMlId())
        .build());
    return dto;
  }

  public ModelDTO expand(ModelDTO dto, ResourceRequest resourceRequest) {
    if (resourceRequest != null && resourceRequest.contains(ResourceRequest.Name.MODELS)) {
      dto.setExpand(true);
    }
    return dto;
  }

  //Build collection
  public ModelDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project)
      throws GenericException {
    ModelDTO dto = new ModelDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);
    dto.setCount(0l);

    validatePagination(resourceRequest);

    if(dto.isExpand()) {
      ProvFileStateParamBuilder provFilesParamBuilder = buildModelProvenanceParams(project, resourceRequest);
      FileStateDTO.PList fileState = null;
      try {
        fileState = provenanceController.provFileStateList(project, provFilesParamBuilder);
        List<FileState> models = fileState.getItems();
        dto.setCount(fileState.getCount());
        for(FileState fileProvStateHit: models) {
          ModelDTO modelDTO = build(uriInfo, resourceRequest, project, fileProvStateHit);
          if(modelDTO != null) {
            dto.addItem(modelDTO);
          }
        }
      } catch(ServiceException se) {
        LOGGER.log(Level.WARNING, "Could not find elastic mapping for models query", se);
        if(RESTCodes.ServiceErrorCode.ELASTIC_QUERY_NO_MAPPING.getCode().equals(se.getErrorCode().getCode())) {
          return dto;
        }
      }
    }
    return dto;
  }

  //Build specific
  public ModelDTO build(UriInfo uriInfo, ResourceRequest resourceRequest, Project project,
                             FileState fileProvenanceHit) {

    ModelDTO modelDTO = new ModelDTO();
    uri(modelDTO, uriInfo, project, fileProvenanceHit);
    expand(modelDTO, resourceRequest);

    if (modelDTO.isExpand()) {
      if(fileProvenanceHit.getXattrs() != null && fileProvenanceHit.getXattrs().containsKey("model_summary")) {
        JSONObject summary = new JSONObject(fileProvenanceHit.getXattrs().get("model_summary"));
        ModelSummary modelSummary = modelSummaryConverter.unmarshalDescription(summary.toString());
        modelDTO.setId(fileProvenanceHit.getMlId());
        modelDTO.setName(modelSummary.getName());
        modelDTO.setVersion(modelSummary.getVersion());
        modelDTO.setUserFullName(modelSummary.getUserFullName());
        modelDTO.setCreated(DateUtils.millis2LocalDateTime(
            Long.parseLong(fileProvenanceHit.getCreateTime().toString())).toString());
        modelDTO.setMetrics(modelSummary.getMetrics());
        modelDTO.setDescription(modelSummary.getDescription());
      } else {
        return null;
      }
    }
    return modelDTO;
  }

  private void buildFilter(ProvFileStateParamBuilder provFilesParamBuilder,
                                            Set<? extends AbstractFacade.FilterBy> filters) {
    if(filters != null) {
      for (AbstractFacade.FilterBy filterBy : filters) {
        if(filterBy.getParam().compareToIgnoreCase(Filters.NAME_EQ.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("model_summary.name", filterBy.getValue());
          provFilesParamBuilder.withXAttrs(map);
        } else if(filterBy.getParam().compareToIgnoreCase(Filters.NAME_LIKE.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("model_summary.name", filterBy.getValue());
          provFilesParamBuilder.withXAttrsLike(map);
        } else {
          throw new WebApplicationException("Filter by need to set a valid filter parameter, but found: " +
              filterBy.getParam(), Response.Status.NOT_FOUND);
        }
      }
    }
  }

  private void buildSortOrder(ProvFileStateParamBuilder provFilesParamBuilder, Set<?
      extends AbstractFacade.SortBy> sort) {
    if(sort != null) {
      for(AbstractFacade.SortBy sortBy: sort) {
        if(sortBy.getValue().compareToIgnoreCase(SortBy.NAME.name()) == 0) {
          provFilesParamBuilder.sortBy("model_summary.name", SortOrder.valueOf(sortBy.getParam().getValue()));
        } else {
          String sortKeyName = sortBy.getValue();
          String sortKeyOrder = sortBy.getParam().getValue();
          provFilesParamBuilder.sortBy("model_summary.metrics." + sortKeyName, SortOrder.valueOf(sortKeyOrder));
        }
      }
    }
  }

  private void validatePagination(ResourceRequest resourceRequest) {
    if(resourceRequest.getLimit() == null || resourceRequest.getLimit() <= 0) {
      resourceRequest.setLimit(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
    }

    if(resourceRequest.getOffset() == null || resourceRequest.getOffset() <= 0) {
      resourceRequest.setOffset(0);
    }
  }

  protected enum SortBy {
    NAME
  }

  protected enum Filters {
    NAME_EQ,
    NAME_LIKE
  }

  private ProvFileStateParamBuilder buildModelProvenanceParams(Project project, ResourceRequest resourceRequest)
      throws GenericException {

    ProvFileStateParamBuilder builder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.MODEL.name())
        .withPagination(resourceRequest.getOffset(), resourceRequest.getLimit());

    buildSortOrder(builder, resourceRequest.getSort());
    buildFilter(builder, resourceRequest.getFilter());
    return builder;
  }
}