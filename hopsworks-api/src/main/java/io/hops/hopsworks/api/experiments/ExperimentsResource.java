package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.experiments.tensorboard.TensorBoardResource;
import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jobs.JobDTO;
import io.hops.hopsworks.api.jobs.JobsBeanParam;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.api.project.util.DsPath;
import io.hops.hopsworks.api.project.util.PathValidator;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.experiments.ExperimentsController;
import io.hops.hopsworks.common.experiments.dto.ExperimentConfiguration;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.swagger.annotations.ApiOperation;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.Consumes;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.logging.Logger;


@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsResource {

  private static final Logger LOGGER = Logger.getLogger(ExperimentsResource.class.getName());

  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private ExperimentsBuilder experimentsBuilder;
  @EJB
  private JWTHelper jwtHelper;
  @Inject
  private TensorBoardResource tensorBoardResource;
  @EJB
  private ExperimentsController experimentsController;
  @EJB
  private PathValidator pathValidator;

  private Project project;
  public ExperimentsResource setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
    return this;
  }

  @ApiOperation(value = "Get a list of all experiments for this project", response = JobDTO.class)
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response getAll(
          @BeanParam Pagination pagination,
          @BeanParam JobsBeanParam jobsBeanParam,
          @Context UriInfo uriInfo) throws ServiceException, ProjectException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.EXPERIMENTS);
    resourceRequest.setOffset(pagination.getOffset());
    resourceRequest.setLimit(pagination.getLimit());
    resourceRequest.setSort(jobsBeanParam.getSortBySet());
    resourceRequest.setFilter(jobsBeanParam.getFilter());
    resourceRequest.setExpansions(jobsBeanParam.getExpansions().getResources());
    ExperimentDTO dto = experimentsBuilder.build(uriInfo, resourceRequest, project);
    return Response.ok().entity(dto).build();
  }

  @ApiOperation( value = "Create or update an experiment", response = JobDTO.class)
  @POST
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response post (
        @PathParam("id") String id,
        ExperimentConfiguration experimentConfiguration,
        @Context HttpServletRequest req,
        @Context UriInfo uriInfo) throws DatasetException, ProjectException {
    if (experimentConfiguration == null) {
      throw new IllegalArgumentException("Experiment configuration was not provided.");
    }
    DsPath dsPath = pathValidator.validatePath(this.project, "Experiments/" + id);
    String fullPath = dsPath.getFullPath().toString();

    experimentsController.publish(fullPath, experimentConfiguration);
    return Response.ok().entity(experimentConfiguration).build();
  }

  @ApiOperation(value = "TensorBoard sub-resource", tags = {"TensorBoardResource"})
  @Path("{id}/tensorboard")
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  public TensorBoardResource tensorboard(@PathParam("id") String id) {
    return this.tensorBoardResource.setProject(project, id);
  }
}
