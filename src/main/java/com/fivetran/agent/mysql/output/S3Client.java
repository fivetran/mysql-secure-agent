/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.amazonaws.services.s3.AmazonS3Client;

import java.io.File;
import java.util.Optional;

public class S3Client implements BucketClient {
    private final String bucketName;
    private final AmazonS3Client client;
    private final Optional<String> prefix;

    public S3Client(String bucketName) {
        this(bucketName, Optional.empty());
    }

    public S3Client(String bucketName, String prefix) {
        this(bucketName, Optional.of(prefix));
    }

    private S3Client(String bucketName, Optional<String> prefix) {
        this.bucketName = bucketName;
        this.prefix = prefix;
        this.client = new AmazonS3Client();
    }

    @Override
    public void copy(String subdirectory, File file) {
        client.putObject(
                bucketName,
                prefix.orElse("") + (!subdirectory.isEmpty()? subdirectory + "/" : "") + file.getName(),
                file);
    }
}
