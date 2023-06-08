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
package io.trino.filesystem.hdfs.cache;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class CachingFileSystemConfig
{
    private boolean cacheEnabled;
    private String baseDirectory;
    private DataSize maxCacheSize = DataSize.of(2, GIGABYTE);
    private String filterClass;
    private String filterConfigFile;

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config("cache.enabled")
    @ConfigDescription("Cache data file to workers' local storage")
    public CachingFileSystemConfig setCacheEnabled(boolean value)
    {
        this.cacheEnabled = value;
        return this;
    }

    @Nullable
    public String getBaseDirectory()
    {
        return baseDirectory;
    }

    @Config("cache.base-directory")
    @ConfigDescription("Base URI to cache data")
    public CachingFileSystemConfig setBaseDirectory(String baseDirectory)
    {
        this.baseDirectory = baseDirectory;
        return this;
    }

    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.max-cache-size")
    @ConfigDescription("The maximum cache size available for cache")
    public CachingFileSystemConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    @Nullable
    public String getFilterClass()
    {
        return filterClass;
    }

    @Config("cache.filter-class")
    @ConfigDescription("The implementation class of CacheFilter")
    public CachingFileSystemConfig setFilterClass(String filterClass)
    {
        this.filterClass = filterClass;
        return this;
    }

    @Nullable
    public String getFilterConfigFile()
    {
        return filterConfigFile;
    }

    @Config("cache.filter-config-file")
    @ConfigDescription("The alluxio cache filter config file")
    public CachingFileSystemConfig setFilterConfigFile(String filterConfigFile)
    {
        this.filterConfigFile = filterConfigFile;
        return this;
    }
}
