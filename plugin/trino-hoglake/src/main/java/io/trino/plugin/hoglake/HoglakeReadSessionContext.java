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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

/**
 * Verified claims and signed storage grants for a dedicated read session.
 * This context binds one scoped caller to one catalog and its storage grants.
 */
final class HoglakeReadSessionContext
{
    static final int MAX_KEY_LENGTH = 1024;
    private static final String S3_SCHEME_PREFIX = "s3://";
    private static final Pattern BUCKET = Pattern.compile("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]");
    private static final Pattern REGION = Pattern.compile("[a-z0-9-]{1,32}");
    private static final Pattern SEGMENT = Pattern.compile("[A-Za-z0-9_.=-]+");

    private final String catalog;
    private final String token;
    private final String principal;
    private final String user;
    private final String organization;
    private final String sessionId;
    private final long generation;
    private final long sessionDeadline;
    private final long teamId;
    private final Placement placement;
    private final StorageSession storage;
    private final List<StorageGrant> grants;

    HoglakeReadSessionContext(
            String catalog,
            String token,
            String principal,
            String user,
            String organization,
            String sessionId,
            long generation,
            long sessionDeadline,
            long teamId,
            Placement placement,
            StorageSession storage,
            List<StorageGrant> grants)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.token = requireNonNull(token, "token is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.user = requireNonNull(user, "user is null");
        this.organization = requireNonNull(organization, "organization is null");
        this.sessionId = requireNonNull(sessionId, "sessionId is null");
        this.generation = generation;
        this.sessionDeadline = sessionDeadline;
        this.teamId = teamId;
        this.placement = requireNonNull(placement, "placement is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.grants = ImmutableList.copyOf(grants);
    }

    public String catalog()
    {
        return catalog;
    }

    public String token()
    {
        return token;
    }

    public String principal()
    {
        return principal;
    }

    public String user()
    {
        return user;
    }

    public String organization()
    {
        return organization;
    }

    public String sessionId()
    {
        return sessionId;
    }

    public long generation()
    {
        return generation;
    }

    public long sessionDeadline()
    {
        return sessionDeadline;
    }

    public long teamId()
    {
        return teamId;
    }

    public Placement placement()
    {
        return placement;
    }

    public StorageSession storage()
    {
        return storage;
    }

    public List<StorageGrant> grants()
    {
        return grants;
    }

    /**
     * Return true when any signed grant authorizes the requested object.
     */
    public boolean allowsStorageTarget(String bucket, String region, String objectKey)
    {
        if (!isValidStorageTarget(bucket, region, objectKey)) {
            return false;
        }
        if (!storage.region().equals(region)) {
            return false;
        }
        for (StorageGrant grant : grants) {
            if (grant.matchesValidated(bucket, region, objectKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true when a signed grant for the named target authorizes the object.
     */
    public boolean allowsStorageTarget(String target, String bucket, String region, String objectKey)
    {
        if (target == null || target.isBlank() || !isValidStorageTarget(bucket, region, objectKey)) {
            return false;
        }
        if (!storage.region().equals(region)) {
            return false;
        }
        for (StorageGrant grant : grants) {
            if (grant.target().equals(target) && grant.matchesValidated(bucket, region, objectKey)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true when any signed grant authorizes the requested S3 URI.
     */
    public boolean allowsStorageLocation(String location, String region)
    {
        StorageLocation parsed = parseS3Location(location);
        if (parsed == null) {
            return false;
        }
        return allowsStorageTarget(parsed.bucket(), region, parsed.objectKey());
    }

    /**
     * Return true when a signed grant for the named target authorizes the S3 URI.
     */
    public boolean allowsStorageLocation(String target, String location, String region)
    {
        StorageLocation parsed = parseS3Location(location);
        if (parsed == null) {
            return false;
        }
        return allowsStorageTarget(target, parsed.bucket(), region, parsed.objectKey());
    }

    /**
     * Refuse access when no signed grant authorizes the requested object.
     */
    public void requireStorageTarget(String bucket, String region, String objectKey)
    {
        if (!allowsStorageTarget(bucket, region, objectKey)) {
            throw targetDenied();
        }
    }

    /**
     * Refuse access when no signed grant for the target authorizes the object.
     */
    public void requireStorageTarget(String target, String bucket, String region, String objectKey)
    {
        if (!allowsStorageTarget(target, bucket, region, objectKey)) {
            throw targetDenied();
        }
    }

    /**
     * Refuse access when no signed grant authorizes the requested S3 URI.
     */
    public void requireStorageLocation(String location, String region)
    {
        if (!allowsStorageLocation(location, region)) {
            throw targetDenied();
        }
    }

    /**
     * Refuse access when no signed grant for the target authorizes the S3 URI.
     */
    public void requireStorageLocation(String target, String location, String region)
    {
        if (!allowsStorageLocation(target, location, region)) {
            throw targetDenied();
        }
    }

    @Override
    public String toString()
    {
        return "HoglakeReadSessionContext{redacted}";
    }

    static boolean isValidBucket(String bucket)
    {
        return bucket != null && BUCKET.matcher(bucket).matches() && !bucket.contains("..");
    }

    static boolean isValidRegion(String region)
    {
        return region != null && REGION.matcher(region).matches();
    }

    static boolean isValidKey(String key, boolean prefix)
    {
        if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH) {
            return false;
        }
        if (prefix != key.endsWith("/")) {
            return false;
        }
        String normalized = prefix ? key.substring(0, key.length() - 1) : key;
        if (normalized.isEmpty()) {
            return false;
        }
        for (String segment : normalized.split("/", -1)) {
            if (segment.equals(".") || segment.equals("..") || !SEGMENT.matcher(segment).matches()) {
                return false;
            }
        }
        return true;
    }

    private static boolean isValidStorageTarget(String bucket, String region, String objectKey)
    {
        return isValidBucket(bucket) && isValidRegion(region) && isValidKey(objectKey, false);
    }

    private static StorageLocation parseS3Location(String location)
    {
        if (location == null || !location.startsWith(S3_SCHEME_PREFIX)) {
            return null;
        }
        String remainder = location.substring(S3_SCHEME_PREFIX.length());
        int separator = remainder.indexOf('/');
        if (separator <= 0 || separator >= remainder.length() - 1) {
            return null;
        }
        return new StorageLocation(remainder.substring(0, separator), remainder.substring(separator + 1));
    }

    private static TrinoException targetDenied()
    {
        // A denial hides tokens, credentials, buckets, and object keys.
        return new TrinoException(PERMISSION_DENIED, "Dedicated Hoglake read session does not allow this storage target");
    }

    private record StorageLocation(String bucket, String objectKey) {}

    /**
     * One verified Hoglite placement for the primary catalog.
     */
    record Placement(UUID backendId, String tenant)
    {
        Placement
        {
            requireNonNull(backendId, "backendId is null");
            requireNonNull(tenant, "tenant is null");
        }

        @Override
        public String toString()
        {
            return "Placement{redacted}";
        }
    }

    /**
     * One verified AWS temporary credential bundle from the signed token.
     */
    record StorageSession(String accessKeyId, String secretAccessKey, String sessionToken, String region, long expiresAt)
    {
        StorageSession
        {
            requireNonNull(accessKeyId, "accessKeyId is null");
            requireNonNull(secretAccessKey, "secretAccessKey is null");
            requireNonNull(sessionToken, "sessionToken is null");
            requireNonNull(region, "region is null");
        }

        @Override
        public String toString()
        {
            return "StorageSession{redacted}";
        }
    }

    /**
     * One verified read-only S3 grant from the signed token.
     */
    record StorageGrant(String catalog, String target, String bucket, String region, String path, boolean prefix)
    {
        StorageGrant
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(target, "target is null");
            requireNonNull(bucket, "bucket is null");
            requireNonNull(region, "region is null");
            requireNonNull(path, "path is null");
        }

        private boolean matchesValidated(String requestedBucket, String requestedRegion, String objectKey)
        {
            if (!bucket.equals(requestedBucket) || !region.equals(requestedRegion)) {
                return false;
            }
            if (prefix) {
                return objectKey.startsWith(path);
            }
            return objectKey.equals(path);
        }

        @Override
        public String toString()
        {
            return "StorageGrant{redacted}";
        }
    }
}
