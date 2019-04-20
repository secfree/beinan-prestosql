package com.twitter.presto.gateway.cluster;

import com.google.common.collect.ImmutableList;
import com.twitter.presto.gateway.GatewayConfig;
import com.twitter.presto.gateway.RequestInfo;

import javax.inject.Inject;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ServerSetClusterManager
        implements ClusterManager
{
    private Map<QueryCategory, ServerSetMonitor> monitors = new HashMap();

    @Inject
    public ServerSetClusterManager(GatewayConfig config)
    {
        String zkServer = config.getZookeeperUri();
        String rootPath = config.getZookeeperPath();
        for (QueryCategory category : QueryCategory.values()) {
            monitors.put(category, createServerSetMonitor(category, zkServer, rootPath));
        }
    }

    @Override
    public Optional<URI> getPrestoCluster(RequestInfo request)
    {
        return Optional.empty();
    }

    @Override
    public List<URI> getAllClusters()
    {
        ImmutableList.Builder<URI> builder = ImmutableList.builder();
        for (ServerSetMonitor monitor : monitors.values()) {
            monitor.getServers().stream()
                    .forEach(server -> builder.add(URI.create(server.toString())));
        }
        return builder.build();
    }

    private static ServerSetMonitor createServerSetMonitor(QueryCategory category, String zookeeperUri, String rootPath)
    {
        return new ServerSetMonitor(zookeeperUri, rootPath + '/' + category.toString().toLowerCase());
    }
}
