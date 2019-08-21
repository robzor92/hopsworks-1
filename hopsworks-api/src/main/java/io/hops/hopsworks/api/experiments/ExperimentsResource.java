package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.experiments.provenance.ExperimentFileProvenanceResource;
import io.hops.hopsworks.api.experiments.results.ExperimentResultsResource;
import io.hops.hopsworks.api.experiments.tensorboard.TensorBoardResource;
import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.api.project.util.PathValidator;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.experiments.ExperimentsController;
import io.hops.hopsworks.common.experiments.dto.ExperimentDescription;
import io.hops.hopsworks.common.experiments.dto.ExperimentDTO;
import io.hops.hopsworks.common.hdfs.HdfsUsersController;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.exceptions.DatasetException;
import io.hops.hopsworks.exceptions.ExperimentsException;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.ApiOperation;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.util.Collection;
import java.util.logging.Level;
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
  @Inject
  private ExperimentResultsResource resultsResource;
  @Inject
  private ExperimentFileProvenanceResource provenanceResource;
  @EJB
  private ExperimentsController experimentsController;
  @EJB
  private PathValidator pathValidator;
  @EJB
  private HdfsUsersController hdfsUsersController;
  @EJB
  private ProvenanceController provenanceController;


  private Project project;
  public ExperimentsResource setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
    return this;
  }

  @ApiOperation(value = "Get a list of all experiments for this project", response = ExperimentDTO.class)
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response getAll(
      @BeanParam Pagination pagination,
      @BeanParam ExperimentsBeanParam experimentsBeanParam,
      @Context UriInfo uriInfo) throws ServiceException, GenericException, ExperimentsException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.EXPERIMENTS);
    resourceRequest.setOffset(pagination.getOffset());
    resourceRequest.setLimit(pagination.getLimit());
    resourceRequest.setFilter(experimentsBeanParam.getFilter());
    resourceRequest.setSort(experimentsBeanParam.getSortBySet());
    resourceRequest.setExpansions(experimentsBeanParam.getExpansions().getResources());
    ExperimentDTO dto = experimentsBuilder.build(uriInfo, resourceRequest, project);
    return Response.ok().entity(dto).build();
  }

  @ApiOperation( value = "Get an experiment", response = ExperimentDTO.class)
  @GET
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response get (
      @PathParam("id") String id,
      @Context UriInfo uriInfo,
      @BeanParam ExperimentsBeanParam experimentsBeanParam)
      throws ServiceException, GenericException, ExperimentsException {
    ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.EXPERIMENTS);
    resourceRequest.setExpansions(experimentsBeanParam.getExpansions().getResources());

    ProvFileStateParamBuilder provFilesParamBuilder = new ProvFileStateParamBuilder()
        .withProjectInodeId(project.getInode().getId())
        .withMlType(Provenance.MLType.EXPERIMENT.name())
        .withAppState()
        .withMlId(id);

    GenericEntity<Collection<FileState>> fileProvenanceHits = new GenericEntity<Collection<FileState>>(
        provenanceController.provFileStateList(provFilesParamBuilder)) {
    };

    if(!fileProvenanceHits.getEntity().isEmpty()) {
      ExperimentDTO dto = experimentsBuilder.build(uriInfo, resourceRequest, project,
          fileProvenanceHits.getEntity().iterator().next());
      return Response.ok().entity(dto).build();
    } else {
      throw new ExperimentsException(RESTCodes.ExperimentsErrorCode.EXPERIMENT_NOT_FOUND, Level.FINE);
    }
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
      ExperimentDescription experimentDescription,
      @QueryParam("xattr") ExperimentDTO.XAttrSetFlag xAttrSetFlag,
      @QueryParam("model") String model,
      @Context HttpServletRequest req,
      @Context UriInfo uriInfo,
      @Context SecurityContext sc) throws DatasetException {

    if (experimentDescription == null && model == null) {
      throw new IllegalArgumentException("Experiment configuration or model was not provided");
    }
    Users user = jwtHelper.getUserPrincipal(sc);
    if(experimentDescription != null) {
      experimentsController.attachExperiment(id, project, user, experimentDescription, xAttrSetFlag);
    } else {
      experimentsController.attachModel(id, project, model, xAttrSetFlag);
    }
    return Response.ok().entity(experimentDescription).build();
  }

  @ApiOperation( value = "Delete an experiment")
  @DELETE
  @Path("{id}")
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
  public Response delete (
      @PathParam("id") String id,
      @Context HttpServletRequest req,
      @Context UriInfo uriInfo,
      @Context SecurityContext sc) {

    Users hopsworksUser = jwtHelper.getUserPrincipal(sc);
    String hdfsUser = hdfsUsersController.getHdfsUserName(project, hopsworksUser);
    experimentsController.delete(id, project, hdfsUser);
    return Response.ok().build();
  }

  @ApiOperation(value = "TensorBoard sub-resource", tags = {"TensorBoardResource"})
  @Path("{id}/tensorboard")
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  public TensorBoardResource tensorboard(@PathParam("id") String id) {
    return this.tensorBoardResource.setProject(project, id);
  }

  @ApiOperation(value = "Results sub-resource", tags = {"ExperimentResultsResource"})
  @Path("{id}/results")
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  public ExperimentResultsResource results(@PathParam("id") String id) {
    return this.resultsResource.setProject(project, id);
  }

  @ApiOperation(value = "Provenance sub-resource", tags = {"ExperimentFileProvenanceResource"})
  @Path("{id}/provenance")
  @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
  public ExperimentFileProvenanceResource provenance(@PathParam("id") String id) {
    return this.provenanceResource.setProject(project, id);
  }
}
