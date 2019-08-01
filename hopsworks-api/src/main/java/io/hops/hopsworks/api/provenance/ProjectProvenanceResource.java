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
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.provenance.ProvAppFootprintType;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByFile;
import io.hops.hopsworks.common.provenance.ProvFileHit;
import io.hops.hopsworks.common.provenance.ProvFileOpHit;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.ProvFileStateHit;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByFile;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.SimpleResult;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.jwt.annotation.JWTRequired;
import io.hops.hopsworks.restutils.RESTCodes;
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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
  private ElasticController elasticCtrl;
  @EJB
  private ProjectFacade projectFacade;
  
  private Project project;

  public void setProjectId(Integer projectId) {
    this.project = projectFacade.find(projectId);
  }
  
  @GET
  @Path("list")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFiles(
    @BeanParam ProvFileDetailsQueryParamsBean fileDetails,
    @BeanParam ProvMLAssetListQueryParamsBean mlAssetParams,
    @BeanParam ProvFileAppDetailsQueryParamsBean appDetails,
    @BeanParam QueryDetailsParamsBean queryDetails,
    @Context HttpServletRequest req) throws ServiceException, GenericException, ProjectException {
    logger.log(Level.INFO, "Local content path:{0} file params:{1} ml asset params:{2} " +
        "app details params:{3} query params:{4}",
      new Object[]{req.getRequestURL().toString(), fileDetails, mlAssetParams, appDetails, queryDetails});
    if(queryDetails.isCount()) {
      Long countResult = provenanceCtrl.provFileStateCount(fileDetails.params(project.getId()),
        mlAssetParams.params(), appDetails.params());
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new SimpleResult<>(countResult)).build();
    } else {
      GenericEntity<List<ProvFileStateHit>> searchResults = new GenericEntity<List<ProvFileStateHit>>(
        provenanceCtrl.provFileState(fileDetails.params(project.getId()), mlAssetParams.params(),
          appDetails.params())) {
      };
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(searchResults).build();
    }
  }

  @GET
  @Path("exact")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response getFiles(
    @BeanParam ProvFileQueryParamsBean fileParams,
    @BeanParam ProvMLAssetQueryParamsBean mlAssetParams,
    @BeanParam ProvFileAppDetailsQueryParamsBean appDetailsParams,
    @Context HttpServletRequest req) throws ServiceException, GenericException, ProjectException {
    logger.log(Level.INFO, "Local content path:{0} file params:{1} ml asset params:{2} app details params:{3} ",
      new Object[]{req.getRequestURL().toString(), fileParams, mlAssetParams, appDetailsParams});
    GenericEntity<List<ProvFileStateHit>> searchResults = new GenericEntity<List<ProvFileStateHit>>(
      elasticCtrl.provFileState(
        fileParams.params(project.getId()), mlAssetParams.params(), appDetailsParams.params())) {
    };
    if(searchResults.getEntity().isEmpty()) {
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.NOT_FOUND).build();
    } else if(searchResults.getEntity().size() > 1){
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.INTERNAL_SERVER_ERROR).build();
    } else {
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(searchResults.getEntity().get(0)).build();
    }
  }
  
  @GET
  @Path("file/{inodeId}/history")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response fileOpHistory(
    @PathParam("inodeId") Long inodeId,
    @QueryParam("type") @DefaultValue("FULL") FileOpsReturnType type,
    @QueryParam("withFullPath") @DefaultValue("false") boolean withFullPath,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    logger.log(Level.INFO, "Local content path:{0} inodeId:{1}",
      new Object[]{req.getRequestURL().toString(), inodeId});
    List<ProvFileOpHit> result = provenanceCtrl.provFileOps(inodeId, null, withFullPath);
    switch(type) {
      case FULL:
        GenericEntity<List<ProvFileOpHit>> fullResults = new GenericEntity<List<ProvFileOpHit>>(result) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(fullResults).build();
      case COMPACT:
        GenericEntity<List<ProvFileOpsCompactByApp>> compactResults
          = new GenericEntity<List<ProvFileOpsCompactByApp>>(ProvFileOpsCompactByApp.compact(result)) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(compactResults).build();
      case SUMMARY:
        GenericEntity<List<ProvFileOpsSummaryByApp>> summaryResults
          = new GenericEntity<List<ProvFileOpsSummaryByApp>>(ProvFileOpsSummaryByApp.summary(result)) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(summaryResults).build();
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "footprint type: " + type);
    }
  }
  
  @GET
  @Path("app/{appId}/fileOperations")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response appFileOps(
    @PathParam("appId") String appId,
    @QueryParam("type") @DefaultValue("FULL") FileOpsReturnType type,
    @QueryParam("withFullPath") @DefaultValue("false") boolean withFullPath,
    @Context HttpServletRequest req) throws ServiceException, GenericException {
    logger.log(Level.INFO, "Local content path:{0} appId:{1}",
      new Object[]{req.getRequestURL().toString(), appId});
    List<ProvFileOpHit> result = provenanceCtrl.provFileOps(null, appId, withFullPath);
    switch(type) {
      case FULL:
        GenericEntity<List<ProvFileOpHit>> fullResults = new GenericEntity<List<ProvFileOpHit>>(result) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(fullResults).build();
      case COMPACT:
        GenericEntity<List<ProvFileOpsCompactByFile>> compactResults
          = new GenericEntity<List<ProvFileOpsCompactByFile>>(ProvFileOpsCompactByFile.compact(result)) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(compactResults).build();
      case SUMMARY:
        GenericEntity<List<ProvFileOpsSummaryByFile>> summaryResults
          = new GenericEntity<List<ProvFileOpsSummaryByFile>>(ProvFileOpsSummaryByFile.summary(result)) {};
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(summaryResults).build();
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "footprint type: " + type);
    }
  }
  
  public enum FileOpsReturnType {
    FULL,
    COMPACT,
    SUMMARY
  }
  
  @GET
  @Path("app/{appId}/footprint")
  @Produces(MediaType.APPLICATION_JSON)
  @AllowedProjectRoles({AllowedProjectRoles.DATA_SCIENTIST, AllowedProjectRoles.DATA_OWNER})
  @JWTRequired(acceptedTokens = {Audience.API}, allowedUserRoles = {"HOPS_ADMIN", "HOPS_USER"})
  public Response appProvenance(
    @PathParam("appId") String appId,
    @QueryParam("type") @DefaultValue("ALL") ProvAppFootprintType type,
    @Context HttpServletRequest req) throws ServiceException {
    logger.log(Level.INFO, "Local content path:{0} appId:{1}",
      new Object[]{req.getRequestURL().toString(), appId});
    GenericEntity<List<ProvFileHit>> fullResults = new GenericEntity<List<ProvFileHit>>(
      elasticCtrl.provAppFootprint(appId, type)) {};
    return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(fullResults).build();
  }
  
  public enum FileProvenanceField {
    FILE_INODE_ID,
    PROJECT_INODE_ID,
    DATASET_INODE_ID,
    USER_ID,
    APP_ID,
    FILE_INODE_NAME;
  }

  public static enum AppProvenanceField {
    APP_ID,
    APP_STATE,
    APP_NAME,
    APP_USER;
  }
}
