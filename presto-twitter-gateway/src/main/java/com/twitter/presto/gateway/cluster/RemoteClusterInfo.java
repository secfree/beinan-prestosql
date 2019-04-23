package com.twitter.presto.gateway.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteClusterInfo
        extends RemoteState
{
    private static final Logger log = Logger.get(RemoteClusterInfo.class);

    private final AtomicLong runningQueries = new AtomicLong();
    private final AtomicLong blockedQueries = new AtomicLong();
    private final AtomicLong queuedQueries = new AtomicLong();
    private final AtomicLong activeWorkers = new AtomicLong();

    public RemoteClusterInfo(HttpClient httpClient, URI remoteUri)
    {
        super(httpClient, remoteUri);
    }

    public void handleResponse(JsonNode response)
    {
    }
}
