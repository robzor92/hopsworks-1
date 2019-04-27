package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.models.dto.ModelDTO;
import io.hops.hopsworks.api.models.dto.ModelSummary;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.AbstractFacade;
import io.hops.hopsworks.common.dao.hdfsUser.HdfsUsersFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.user.UserFacade;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
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
      throws ServiceException, GenericException {
    ModelDTO dto = new ModelDTO();
    uri(dto, uriInfo, project);
    expand(dto, resourceRequest);
    dto.setCount(0l);

    validatePagination(resourceRequest);

    if(dto.isExpand()) {
      ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
          .withProjectInodeId(project.getInode().getId())
          .withMlType(Provenance.MLType.MODEL.name())
          .withPagination(resourceRequest.getOffset(), resourceRequest.getLimit())
          .withAppExpansion();

      buildSortOrder(provFilesParamBuilder, resourceRequest.getSort());
      buildFilter(provFilesParamBuilder, resourceRequest.getFilter());

      FileStateDTO.PList fileState = provenanceController.provFileStateList(provFilesParamBuilder);

      List<FileState> models = fileState.getItems();
      dto.setCount(fileState.getCount());

      for(FileState fileProvStateHit: models) {
        ModelDTO modelDTO = build(uriInfo, resourceRequest, project, fileProvStateHit);
        if(modelDTO != null) {
          dto.addItem(modelDTO);
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
      if(fileProvenanceHit.getXattrs().containsKey("model_summary")) {
        JSONObject summary = new JSONObject(fileProvenanceHit.getXattrs().get("model_summary"));
        ModelSummary modelSummary = modelSummaryConverter.unmarshalDescription(summary.toString());
        modelDTO.setName(modelSummary.getName());
        modelDTO.setVersion(modelSummary.getVersion());
        modelDTO.setUserFullName(modelSummary.getUserFullName());
        modelDTO.setParameters(modelSummary.getParameters());
        modelDTO.setOutputs(modelSummary.getOutputs());
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
        if(filterBy.getParam().compareToIgnoreCase(Filters.NAME.name()) == 0) {
          HashMap<String, String> map = new HashMap<>();
          map.put("model_summary.name", filterBy.getValue());
          provFilesParamBuilder.withXAttrsLike(map);
        } else {
          throw new WebApplicationException("Filter by need to set a valid filter parameter, but found: " + filterBy,
              Response.Status.NOT_FOUND);
        }
      }
    }
  }

  public void buildSortOrder(ProvFileStateParamBuilder provFilesParamBuilder, Set<? extends AbstractFacade.SortBy> sort)
  {
    if(sort != null) {
      for(AbstractFacade.SortBy sortBy: sort) {
        if(sortBy.getValue().compareToIgnoreCase(SortBy.NAME.name()) == 0) {
          provFilesParamBuilder.sortBy("model_summary.name", SortOrder.valueOf(sortBy.getParam().getValue()));
        } else {
          throw new WebApplicationException("Sort by need to set a valid sort parameter, but found: " + sortBy,
              Response.Status.NOT_FOUND);
        }
      }
    }
  }

  protected enum SortBy {
    NAME
  }

  protected enum Filters {
    NAME
  }

  private void validatePagination(ResourceRequest resourceRequest) {
    if(resourceRequest.getLimit() == null || resourceRequest.getLimit() <= 0) {
      resourceRequest.setLimit(ElasticController.DEFAULT_PAGE_SIZE);
    }

    if(resourceRequest.getOffset() == null || resourceRequest.getOffset() <= 0) {
      resourceRequest.setOffset(0);
    }
  }
}