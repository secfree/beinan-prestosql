package com.twitter.presto.gateway.cluster;

import com.google.common.collect.ImmutableList;
import com.twitter.presto.gateway.RequestInfo;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public class ServerSetClusterManager
        implements ClusterManager
{
    @Override
    public Optional<URI> getPrestoCluster(RequestInfo request)
    {
        return Optional.empty();
    }

    @Override
    public List<URI> getAllClusters()
    {
        return ImmutableList.of();
    }
}
