/*
 *  Copyright 2021 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.openmetadata.service.resources.custom;

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Data;
import org.openmetadata.schema.api.VoteRequest;
import org.openmetadata.schema.api.data.CreateTable;
import org.openmetadata.schema.api.data.CreateTableProfile;
import org.openmetadata.schema.api.data.RestoreEntity;
import org.openmetadata.schema.api.tests.CreateCustomMetric;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.schema.tests.CustomMetric;
import org.openmetadata.schema.type.*;
import org.openmetadata.schema.type.csv.CsvImportResult;
import org.openmetadata.service.Entity;
import org.openmetadata.service.jdbi3.ListFilter;
import org.openmetadata.service.jdbi3.TableRepository;
import org.openmetadata.service.limits.Limits;
import org.openmetadata.service.resources.Collection;
import org.openmetadata.service.resources.EntityResource;
import org.openmetadata.service.resources.databases.DatabaseUtil;
import org.openmetadata.service.security.Authorizer;
import org.openmetadata.service.security.policyevaluator.OperationContext;
import org.openmetadata.service.security.policyevaluator.ResourceContext;
import org.openmetadata.service.util.FullyQualifiedName;
import org.openmetadata.service.util.JsonUtils;
import org.openmetadata.service.util.ResultList;

import javax.json.JsonPatch;
import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.openmetadata.common.utils.CommonUtil.listOf;

@Path("/v1/custom")
@Tag(
    name = "Custom",
    description =
        "For internal use only. It is used to create custom metrics and test suites for tables.")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Collection(name = "custom")
public class CustomResource {
    private final TableRepository repository;
    public CustomResource() {
        this.repository = (TableRepository) Entity.getEntityRepository(Entity.TABLE);
    }


    @POST
    @Path("/updateLatestTableProfile")
    @Operation(
        operationId = "updateTableCustomProfile",
        summary = "Update Table custom profile.",
        description = "Update Table custom profile.",
        responses = {
                @ApiResponse(
                        responseCode = "200",
                        description = "Successfully updated the Table ",
                        content =
                        @Content(
                                mediaType = "application/json",
                                schema = @Schema(implementation = Table.class)))
        })
    public Table updateLatestTableProfile(
            UpdateTableProfileRequest request) {
        return repository.updateLatestTableProfile(request.getTableId(), request.getCreateTableProfile());
    }

    @Data
    public static class UpdateTableProfileRequest {
        private UUID tableId;
        private CreateTableProfile createTableProfile;
    }
}
