package com.fivetran.agent.mysql.credentials;

import com.fivetran.agent.mysql.config.DatabaseCredentials;

public class Credentials {
    public S3 s3Credentials = new S3();
    public DatabaseCredentials dbCredentials;

    public class S3 {
        public String bucket;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            S3 s3 = (S3) o;

            return bucket != null ? bucket.equals(s3.bucket) : s3.bucket == null;
        }

        @Override
        public int hashCode() {
            return bucket != null ? bucket.hashCode() : 0;
        }
    }

    @Override
    public int hashCode() {
        int result = s3Credentials.hashCode();
        result = 31 * result + dbCredentials.hashCode();
        return result;
    }
}
