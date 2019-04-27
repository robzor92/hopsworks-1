package io.hops.hopsworks.api.models;

import io.hops.hopsworks.api.experiments.ExperimentsBeanParam;
import io.hops.hopsworks.api.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.api.models.dto.ModelDTO;
import io.hops.hopsworks.api.models.dto.ModelSummary;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;

import io.swagger.annotations.ApiOperation;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.util.logging.Logger;


@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ModelsResource {

  private static final Logger LOGGER = Logger.getLogger(ModelsResource.class.getName());

  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private ModelsBuilder modelsBuilder;
  @EJB
  private ModelsController modelsController;
  @EJB
  private JWTHelper jwtHelper;


  private Project project;
  public ModelsResource setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
    return this;
  }

  @ApiOperation(value = "Get a list of all experiments for this project", response = ModelDTO.class)
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response getAll(
      @BeanParam Pagination pagination,
      @BeanParam ExperimentsBeanParam experimentsBeanParam,
      @Context UriInfo uriInfo) throws ServiceException, GenericException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.MODELS);
    resourceRequest.setOffset(pagination.getOffset());
    resourceRequest.setLimit(pagination.getLimit());
    resourceRequest.setFilter(experimentsBeanParam.getFilter());
    resourceRequest.setSort(experimentsBeanParam.getSortBySet());
    resourceRequest.setExpansions(experimentsBeanParam.getExpansions().getResources());
    ModelDTO dto = modelsBuilder.build(uriInfo, resourceRequest, project);
    return Response.ok().entity(dto).build();
  }

  @ApiOperation( value = "Create or update an experiment", response = ExperimentDTO.class)
  @POST
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response post (
      @PathParam("id") String id,
      ModelSummary modelSummary,
      @QueryParam("xattr") ModelDTO.XAttrSetFlag xAttrSetFlag,
      @Context HttpServletRequest req,
      @Context UriInfo uriInfo,
      @Context SecurityContext sc) throws DatasetException {
    if (modelSummary == null) {
      throw new IllegalArgumentException("Experiment configuration or model was not provided");
    }
    Users user = jwtHelper.getUserPrincipal(sc);
    if(modelSummary != null) {
      String realName = user.getFname() + " " + user.getLname();
      modelsController.attachModel(project, realName, modelSummary, xAttrSetFlag);
    }
    UriBuilder builder = uriInfo.getAbsolutePathBuilder().path(id);
    if(xAttrSetFlag.equals(ExperimentDTO.XAttrSetFlag.CREATE)) {
      return Response.created(builder.build()).entity(modelSummary).build();
    } else {
      return Response.ok(builder.build()).entity(modelSummary).build();
    }
  }

}
