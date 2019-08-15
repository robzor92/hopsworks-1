/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.api.filter.AllowedProjectRoles;
import io.hops.hopsworks.api.filter.Audience;
import io.hops.hopsworks.api.filter.NoCacheResponse;
import io.hops.hopsworks.api.provenance.v2.ProvFileOpsBeanParam;
import io.hops.hopsworks.api.provenance.v2.ProvFileStateBeanParam;
import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.provenance.AppFootprintType;
import io.hops.hopsworks.common.provenance.ProvDatasetState;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ProvFileHit;
import io.hops.hopsworks.common.provenance.ProvFileOpHit;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.ProvFileStateHit;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.SimpleResult;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
import io.swagger.annotations.Api;

import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.context.RequestScoped;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RequestScoped
@TransactionAttribute(TransactionAttributeType.NEVER)
@Api(value = "Project Provenance Service", description = "Project Provenance Service")
public class ProjectProvenanceResource {

  private static final Logger logger = Logger.getLogger(ProjectProvenanceResource.class.getName());

  @EJB
  private NoCacheResponse noCacheResponse;
  @EJB
  private ProvenanceController provenanceCtrl;
  @EJB
  private ProjectFacade projectFacade;
  
  private Project project;

  public void setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
  }
  
  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.ANYONE})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getProvenanceStatus()
    throws GenericException {
    Inode.MetaStatus status = provenanceCtrl.getProjectProvenanceStatus(project);
    return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
      .entity(new SimpleResult(status.name())).build();
  }
  
  @POST
  @Path("/status/{status}")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.ANYONE})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response changeProvenanceStatus(
    @PathParam("status") Inode.MetaStatus status)
    throws GenericException {
    provenanceCtrl.changeProjectProvenanceStatus(project, status);
    return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).build();
  }
  
  @GET
  @Path("/content")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.ANYONE})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response content() {
    GenericEntity<List<ProvDatasetState>> result
      = new GenericEntity<List<ProvDatasetState>>(provenanceCtrl.getDatasetsProvenanceStatus(project)) {};
    return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(result).build();
  }
  
  @GET
  @Path("file/state")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileStates(
    @BeanParam ProvFileStateBeanParam params,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    ProvFileStateParamBuilder paramBuilder = new ProvFileStateParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFileState(params.getFileStateParams())
      .withQueryParamExactXAttr(params.getExactXAttrParams())
      .withQueryParamLikeXAttr(params.getLikeXAttrParams())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppState(params.getAppStateParams());
    
    if(params.isCount()) {
      Long countResult = provenanceCtrl.provFileStateCount(paramBuilder);
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new SimpleResult<>(countResult)).build();
    } else {
      Collection<ProvFileStateHit> searchResults = provenanceCtrl.provFileState(paramBuilder).values();
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new GenericEntity<Collection<ProvFileStateHit>>(searchResults) {}).build();
    }
  }
  
  @GET
  @Path("file/ops")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @BeanParam ProvFileOpsBeanParam params,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFileOps(params.getFileOpsFilter());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return getFileOps(paramBuilder, params.isCount(), params.getReturnType());
  }
  
  @GET
  @Path("file/{inodeId}/ops")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @PathParam("inodeId") Long fileInodeId,
    @BeanParam ProvFileOpsBeanParam params,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withFileInodeId(fileInodeId)
      .withQueryParamFileOps(params.getFileOpsFilter());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return getFileOps(paramBuilder, params.isCount(), params.getReturnType());
  }
  
  @GET
  @Path("app/{appId}/footprint/{type}")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response appProvenance(
    @PathParam("appId") String appId,
    @PathParam("type") @DefaultValue("ALL") AppFootprintType footprintType,
    @BeanParam ProvFileOpsBeanParam params,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withAppId(appId)
      .withQueryParamFileOps(params.getFileOpsFilter());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    
    List<ProvFileHit> result = provenanceCtrl.provAppFootprint(paramBuilder, footprintType);
    return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
      .entity(new GenericEntity<List<ProvFileHit>>(result) {}).build();
  }
  
  private Response getFileOps(ProvFileOpsParamBuilder params, boolean count,
    FileOpsReturnType returnType)
    throws ServiceException, GenericException {
    if(count) {
      Long result = provenanceCtrl.provFileOpsCount(params);
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new SimpleResult<>(result)).build();
    } else {
      List<ProvFileOpHit> result = provenanceCtrl.provFileOps(params);
      switch(returnType) {
        case FULL:
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpHit>>(result) {}).build();
        case COMPACT:
          List<ProvFileOpsCompactByApp> compactResults = ProvFileOpsCompactByApp.compact(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsCompactByApp>>(compactResults) {}).build();
        case SUMMARY:
          List<ProvFileOpsSummaryByApp> summaryResults = ProvFileOpsSummaryByApp.summary(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsSummaryByApp>>(summaryResults) {}).build();
        default:
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
            "footprint filterType: " + returnType);
      }
    }
  }
  
  public enum FileOpsReturnType {
    FULL,
    COMPACT,
    SUMMARY
  }
}
