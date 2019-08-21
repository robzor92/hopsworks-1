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
import io.hops.hopsworks.api.util.Pagination;
import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.provenance.AppFootprintType;
import io.hops.hopsworks.common.provenance.ProvDatasetState;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.xml.SimpleResult;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.swagger.annotations.Api;

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
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileStateParamBuilder paramBuilder = new ProvFileStateParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFileStateFilterBy(params.getFileStateParams())
      .withQueryParamFileStateSortBy(params.getFileStateSortBy())
      .withQueryParamExactXAttr(params.getExactXAttrParams())
      .withQueryParamLikeXAttr(params.getLikeXAttrParams())
      .withQueryParamXAttrSortBy(params.getXattrSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return ProvenanceResourceHelper.getFileStates(noCacheResponse, provenanceCtrl, paramBuilder,
      params.getReturnType());
  }
  
  @GET
  @Path("file/ops")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @BeanParam ProvFileOpsBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withQueryParamFilter(params.getFileOpsFilter())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return ProvenanceResourceHelper.getFileOps(noCacheResponse, provenanceCtrl, paramBuilder,
      params.getOpsCompaction(), params.getReturnType());
  }
  
  @GET
  @Path("file/{inodeId}/state")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileStates(
    @PathParam("inodeId") Long fileInodeId,
    @BeanParam ProvFileStateBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws GenericException, ServiceException {
    ProvFileStateParamBuilder paramBuilder = new ProvFileStateParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withFileInodeId(fileInodeId)
      .withQueryParamFileStateFilterBy(params.getFileStateParams())
      .withQueryParamFileStateSortBy(params.getFileStateSortBy())
      .withQueryParamExactXAttr(params.getExactXAttrParams())
      .withQueryParamLikeXAttr(params.getLikeXAttrParams())
      .withQueryParamXAttrSortBy(params.getXattrSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return ProvenanceResourceHelper.getFileStates(noCacheResponse, provenanceCtrl, paramBuilder,
      params.getReturnType());
  }
  
  @GET
  @Path("file/{inodeId}/ops")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFileOps(
    @PathParam("inodeId") Long fileInodeId,
    @BeanParam ProvFileOpsBeanParam params,
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withFileInodeId(fileInodeId)
      .withQueryParamFilter(params.getFileOpsFilter())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withQueryParamExpansions(params.getExpansions())
      .withQueryParamAppExpansionFilter(params.getAppExpansionParams())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    return ProvenanceResourceHelper.getFileOps(noCacheResponse, provenanceCtrl, paramBuilder, params.getOpsCompaction(),
      params.getReturnType());
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
    @BeanParam Pagination pagination,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    ProvFileOpsParamBuilder paramBuilder = new ProvFileOpsParamBuilder()
      .withProjectInodeId(project.getInode().getId())
      .withAppId(appId)
      .withQueryParamFilter(params.getFileOpsFilter())
      .withQueryParamSortBy(params.getFileOpsSortBy())
      .withPagination(pagination.getOffset(), pagination.getLimit());
    logger.log(Level.INFO, "Local content path:{0} file state params:{1} ",
      new Object[]{req.getRequestURL().toString(), params});
    
    return ProvenanceResourceHelper.getAppFootprint(noCacheResponse, provenanceCtrl, paramBuilder, footprintType,
      params.getReturnType());
  }
  
  public enum FileStructReturnType {
    LIST,
    MIN_TREE,
    FULL_TREE,
    COUNT;
  }
  public enum FileOpsCompactionType {
    NONE,
    FILE_COMPACT,
    FILE_SUMMARY
  }
}
