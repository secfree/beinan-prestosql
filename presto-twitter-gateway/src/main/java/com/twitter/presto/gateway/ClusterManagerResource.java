/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.presto.gateway;

import com.google.inject.Inject;
import io.airlift.log.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/clusters")
public class ClusterManagerResource
{
    private static final Logger log = Logger.get(GatewayResource.class);

    private final ClusterManager clusterManager;

    @Inject
    public ClusterManagerResource(ClusterManager clusterManager)
    {
        this.clusterManager = requireNonNull(clusterManager, "clusterManager is null");
    }

    @GET
    public List<URI> getAllClusters()
    {
        return clusterManager.getAllClusters();
    }

    @PUT
    @Path("add")
    @Produces(APPLICATION_JSON)
    public Response addCluster(String uri)
    {
        boolean status = clusterManager.addPrestoCluster(URI.create(uri));

        if (!status) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(format("Presto cluster %s already registered", uri))
                    .build();
        }

        log.info("Presto cluster (%s) added", uri);
        return Response.ok().build();
    }

    @PUT
    @Path("remove")
    @Produces(APPLICATION_JSON)
    public Response removeCluster(String uri)
    {
        boolean status = clusterManager.removePrestoCluster(URI.create(uri));

        if (!status) {
            return Response.status(BAD_REQUEST)
                    .type(TEXT_PLAIN)
                    .entity(format("Presto cluster %s is not registered", uri))
                    .build();
        }

        log.info("Presto cluster (%s) removed", uri);
        return Response.ok().build();
    }
}
