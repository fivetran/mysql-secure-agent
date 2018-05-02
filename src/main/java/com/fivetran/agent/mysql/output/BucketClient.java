/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import java.io.File;

/**
 * Override this in tests for local testing, and also maybe some day we can write an S3 / GCS / Azure / Etc version of this
 */
public interface BucketClient {
    /**
     * Takes a local file maintained by a FileChannel and write it to your remote storage location
     */
    void copy(String prefix, File file);
}
