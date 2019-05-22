package io.hops.hopsworks.api.experiments;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.jobs.JobDTO;
import io.hops.hopsworks.api.jobs.JobsBeanParam;
import io.hops.hopsworks.api.jwt.JWTHelper;
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.api.ResourceRequest;
import io.hops.hopsworks.common.dao.jobs.description.Jobs;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dao.user.Users;
import io.hops.hopsworks.common.experiments.ExperimentConfiguration;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.swagger.annotations.ApiOperation;

import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.logging.Logger;


@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
public class ExperimentsResource {

    private static final Logger LOGGER = Logger.getLogger(io.hops.hopsworks.api.experiments.ExperimentsResource.class.getName());

    @EJB
    private ProjectFacade projectFacade;
    @EJB
    private ExperimentsBuilder experimentsBuilder;
    @EJB
    private JWTHelper jwtHelper;
    @EJB
    private DistributedFsService dfs;

    private Project project;
    public ExperimentsResource setProject(Integer projectId) {
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
            @Context UriInfo uriInfo) {
        ResourceRequest resourceRequest = new ResourceRequest(ResourceRequest.Name.JOBS);
        resourceRequest.setOffset(pagination.getOffset());
        resourceRequest.setLimit(pagination.getLimit());
        resourceRequest.setSort(jobsBeanParam.getSortBySet());
        resourceRequest.setFilter(jobsBeanParam.getFilter());
        resourceRequest.setExpansions(jobsBeanParam.getExpansions().getResources());

        JobDTO dto = experimentsBuilder.build(uriInfo, resourceRequest, project);
        return Response.ok().entity(dto).build();
    }

    @ApiOperation( value = "Create or update an experiment", response = JobDTO.class)
    @PUT
    @Path("{appId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @AllowedProjectRoles({AllowedProjectRoles.DATA_OWNER, AllowedProjectRoles.DATA_SCIENTIST})
    @JWTRequired(acceptedTokens={Audience.API, Audience.JOB}, allowedUserRoles={"HOPS_ADMIN", "HOPS_USER"})
    public Response put (
            @PathParam("appId") String appId,
            ExperimentConfiguration experimentConfiguration,
            @Context HttpServletRequest req,
            @Context UriInfo uriInfo) {
        if (experimentConfiguration == null) {
            throw new IllegalArgumentException("Experiment configuration was not provided.");
        }

        Users user = jwtHelper.getUserPrincipal(req);

        //Check if experiment with same id exists, in that case this is an update and not creation
        Response.Status status = Response.Status.CREATED;
        Jobs job = jobFacade.findByProjectAndName(project, config.getAppName());
        if(job != null) {
            status = Response.Status.OK;
        }
        DistributedFileSystemOps dfsops = dfs.getDfsOps();
        //Set XAttr on directory


        dfsops.setXAttr(appId, "config", null, null);

        ExperimentsDTO dto = new ExperimentsDTO();
        UriBuilder builder = uriInfo.getAbsolutePathBuilder().path(dto.getId());
        if(status == Response.Status.CREATED) {
            return Response.created(builder.build()).entity(dto).build();
        } else {
            return Response.ok(builder.build()).entity(dto).build();
        }
    }
}
