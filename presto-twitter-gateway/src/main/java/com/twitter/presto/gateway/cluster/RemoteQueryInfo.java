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
package com.twitter.presto.gateway.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class RemoteQueryInfo
{
    private static final Logger log = Logger.get(RemoteQueryInfo.class);
    private static final JsonCodec<List<JsonNode>> LIST_JSON_CODEC = listJsonCodec(JsonNode.class);

    private final HttpClient httpClient;
    private final URI queryInfoUri;
    private final AtomicReference<Optional<List<JsonNode>>> queryList = new AtomicReference<>(Optional.empty());
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final AtomicLong lastUpdateNanos = new AtomicLong();
    private final AtomicLong lastWarningLogged = new AtomicLong();

    public RemoteQueryInfo(HttpClient httpClient, URI queryInfoUri)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.queryInfoUri = requireNonNull(queryInfoUri, "queryInfoUri is null");
    }

    public Optional<List<JsonNode>> getQueryList()
    {
        return queryList.get();
    }

    public synchronized void asyncRefresh()
    {
        Duration sinceUpdate = nanosSince(lastUpdateNanos.get());

        if (nanosSince(lastWarningLogged.get()).toMillis() > 1_000 &&
                sinceUpdate.toMillis() > 10_000 &&
                future.get() != null) {
            log.warn("Coordinator update request to %s has not returned in %s", queryInfoUri, sinceUpdate.toString(SECONDS));
            lastWarningLogged.set(System.nanoTime());
        }

        if (sinceUpdate.toMillis() > 1_000 && future.get() == null) {
            Request request = prepareGet()
                    .setUri(queryInfoUri)
                    .build();

            HttpResponseFuture<JsonResponse<List<JsonNode>>> responseFuture = httpClient.executeAsync(request, createFullJsonResponseHandler(LIST_JSON_CODEC));
            future.compareAndSet(null, responseFuture);

            Futures.addCallback(responseFuture, new FutureCallback<JsonResponse<List<JsonNode>>>()
            {
                @Override
                public void onSuccess(@Nullable JsonResponse<List<JsonNode>> result)
                {
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                    if (result != null) {
                        if (result.hasValue()) {
                            queryList.set(Optional.ofNullable(result.getValue()));
                        }
                        if (result.getStatusCode() != OK.code()) {
                            log.warn("Error fetching node state from %s returned status %d: %s", queryInfoUri, result.getStatusCode(), result.getStatusMessage());
                            return;
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t)
                {
                    log.warn("Error fetching query infos from %s: %s", queryInfoUri, t.getMessage());
                    lastUpdateNanos.set(System.nanoTime());
                    future.compareAndSet(responseFuture, null);
                }
            }, directExecutor());
        }
    }
}
