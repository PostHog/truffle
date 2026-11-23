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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHoglakeDedicatedConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HoglakeDedicatedConfig.class)
                .setUri(null)
                .setRequestTimeout(new Duration(120, SECONDS))
                .setLocalRead(false)
                .setJwksFile(null)
                .setIssuer(null)
                .setProfile(HoglakeReadSessionVerifier.DEFAULT_PROFILE)
                .setRemoteTargets(ImmutableSet.of())
                .setHogliteBackends(ImmutableSet.of())
                .setClockSkew(new Duration(60, SECONDS))
                .setMaxLifetime(new Duration(900, SECONDS)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        assertFullMapping(ImmutableMap.<String, String>builder()
                        .put("hoglake.uri", "https://metadata.example.test")
                        .put("hoglake.client.request-timeout", "30s")
                        .put("hoglake.dedicated.local-read", "true")
                        .put("hoglake.dedicated.jwks-file", "/tmp/synthetic-dedicated-jwks.json")
                        .put("hoglake.dedicated.issuer", "https://issuer.example.test")
                        .put("hoglake.dedicated.profile", "custom.aws-session")
                        .put("hoglake.dedicated.remote-targets", "gigahog:posthog,gigahog:imports")
                        .put("hoglake.dedicated.hoglite-backends", "11111111-2222-4333-8444-555555555555")
                        .put("hoglake.dedicated.clock-skew", "15s")
                        .put("hoglake.dedicated.max-lifetime", "600s")
                        .buildOrThrow(),
                new HoglakeDedicatedConfig()
                        .setUri(URI.create("https://metadata.example.test"))
                        .setRequestTimeout(new Duration(30, SECONDS))
                        .setLocalRead(true)
                        .setJwksFile(new File("/tmp/synthetic-dedicated-jwks.json"))
                        .setIssuer("https://issuer.example.test")
                        .setProfile("custom.aws-session")
                        .setRemoteTargets(ImmutableSet.of("gigahog:posthog", "gigahog:imports"))
                        .setHogliteBackends(ImmutableSet.of("11111111-2222-4333-8444-555555555555"))
                        .setClockSkew(new Duration(15, SECONDS))
                        .setMaxLifetime(new Duration(600, SECONDS)));
    }

    @Test
    void testMandatorySettings()
    {
        assertThatThrownBy(() -> new ConfigurationFactory(Map.of("hoglake.dedicated.local-read", "true")).build(HoglakeDedicatedConfig.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testUriRefusalIsRedacted()
    {
        assertThatThrownBy(() -> new HoglakeDedicatedConfig().setUri(URI.create("https://synthetic-secret@metadata.example.test")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid dedicated Hoglake URI")
                .hasNoCause();
    }
}
