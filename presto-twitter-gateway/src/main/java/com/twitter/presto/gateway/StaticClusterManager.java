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

import com.google.common.collect.Iterators;
import com.google.inject.Inject;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toSet;

public class StaticClusterManager
        implements ClusterManager
{
    @GuardedBy("this")
    private Set<URI> clusters;

    @GuardedBy("this")
    private Iterator<URI> iterator;

    @Inject
    public StaticClusterManager(GatewayConfig config)
    {
        clusters = config.getClusters().stream().collect(toSet());
        iterator = Iterators.cycle(clusters);
    }

    @Override
    public URI getPrestoCluster(RequestInfo request)
    {
        return iterator.next();
    }

    @Override
    public List<URI> getAllClusters()
    {
        return clusters.stream().collect(toImmutableList());
    }

    @Override
    public boolean addPrestoCluster(URI cluster)
    {
        boolean status = this.clusters.add(cluster);
        if (status) {
            this.iterator = Iterators.cycle(this.clusters);
        }

        return status;
    }

    @Override
    public boolean removePrestoCluster(URI cluster)
    {
        boolean status = this.clusters.remove(cluster);
        if (status) {
            this.iterator = Iterators.cycle(this.clusters);
        }

        return status;
    }
}
