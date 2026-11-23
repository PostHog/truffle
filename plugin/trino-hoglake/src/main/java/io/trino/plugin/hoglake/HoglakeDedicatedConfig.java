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
package io.trino.plugin.hoglake;

import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Configuration for the default-off dedicated local table read connector.
 */
public class HoglakeDedicatedConfig
{
    private URI uri;
    private Duration requestTimeout = new Duration(120, SECONDS);
    private boolean localRead;
    private File jwksFile;
    private String issuer;
    private String profile = HoglakeReadSessionVerifier.DEFAULT_PROFILE;
    private Set<String> remoteTargets = ImmutableSet.of();
    private Set<String> hogliteBackends = ImmutableSet.of();
    private Duration clockSkew = new Duration(60, SECONDS);
    private Duration maxLifetime = new Duration(900, SECONDS);

    @NotNull
    public URI getUri()
    {
        return uri;
    }

    @Config("hoglake.uri")
    @ConfigDescription("Hoglake REST base URI for dedicated local read requests")
    public HoglakeDedicatedConfig setUri(URI uri)
    {
        if (uri != null && ((!"http".equals(uri.getScheme()) && !"https".equals(uri.getScheme())) ||
                uri.getHost() == null || uri.getRawUserInfo() != null || uri.getRawQuery() != null || uri.getRawFragment() != null)) {
            throw new IllegalArgumentException("Invalid dedicated Hoglake URI");
        }
        this.uri = uri;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("hoglake.client.request-timeout")
    @ConfigDescription("Timeout for each dedicated Hoglake metadata request")
    public HoglakeDedicatedConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public boolean isLocalRead()
    {
        return localRead;
    }

    @Config("hoglake.dedicated.local-read")
    @ConfigDescription("Enable dedicated local table reads with offline read session verification")
    public HoglakeDedicatedConfig setLocalRead(boolean localRead)
    {
        this.localRead = localRead;
        return this;
    }

    @NotNull
    public File getJwksFile()
    {
        return jwksFile;
    }

    @Config("hoglake.dedicated.jwks-file")
    @ConfigDescription("Mounted public JWKS file for offline read session verification")
    public HoglakeDedicatedConfig setJwksFile(File jwksFile)
    {
        this.jwksFile = jwksFile;
        return this;
    }

    @NotNull
    public String getIssuer()
    {
        return issuer;
    }

    @Config("hoglake.dedicated.issuer")
    @ConfigDescription("Exact issuer accepted for dedicated read session tokens")
    public HoglakeDedicatedConfig setIssuer(String issuer)
    {
        this.issuer = issuer;
        return this;
    }

    @NotNull
    public String getProfile()
    {
        return profile;
    }

    @Config("hoglake.dedicated.profile")
    @ConfigDescription("Exact token profile accepted for dedicated read sessions")
    public HoglakeDedicatedConfig setProfile(String profile)
    {
        this.profile = profile;
        return this;
    }

    @NotNull
    public Set<String> getRemoteTargets()
    {
        return remoteTargets;
    }

    @Config("hoglake.dedicated.remote-targets")
    @ConfigDescription("Configured remote storage grant target names for dedicated read sessions")
    public HoglakeDedicatedConfig setRemoteTargets(Set<String> remoteTargets)
    {
        this.remoteTargets = remoteTargets == null ? ImmutableSet.of() : ImmutableSet.copyOf(remoteTargets);
        return this;
    }

    @NotNull
    public Set<String> getHogliteBackends()
    {
        return hogliteBackends;
    }

    @Config("hoglake.dedicated.hoglite-backends")
    @ConfigDescription("Canonical Hoglite backend UUIDs accepted in dedicated read sessions")
    public HoglakeDedicatedConfig setHogliteBackends(Set<String> hogliteBackends)
    {
        this.hogliteBackends = hogliteBackends == null ? ImmutableSet.of() : ImmutableSet.copyOf(hogliteBackends);
        return this;
    }

    @NotNull
    @MinDuration("0s")
    @MaxDuration("600s")
    public Duration getClockSkew()
    {
        return clockSkew;
    }

    @Config("hoglake.dedicated.clock-skew")
    @ConfigDescription("Clock tolerance for dedicated read session verification, from 0s through 600s")
    public HoglakeDedicatedConfig setClockSkew(Duration clockSkew)
    {
        this.clockSkew = clockSkew;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    @MaxDuration("900s")
    public Duration getMaxLifetime()
    {
        return maxLifetime;
    }

    @Config("hoglake.dedicated.max-lifetime")
    @ConfigDescription("Maximum dedicated read session lifetime, from 1s through 900s")
    public HoglakeDedicatedConfig setMaxLifetime(Duration maxLifetime)
    {
        this.maxLifetime = maxLifetime;
        return this;
    }
}
