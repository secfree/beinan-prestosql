package com.twitter.presto.gateway.cluster;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

public class ServerSetMonitor
    implements PathChildrenCacheListener
{


    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws Exception
    {

    }
}
