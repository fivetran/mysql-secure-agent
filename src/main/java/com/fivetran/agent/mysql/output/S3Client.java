/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;
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
        List<Tag> dataFileTag = ImmutableList.of(new Tag("data_file", "true"));
        String fullPath = prefix.orElse("") + (!subdirectory.isEmpty() ? subdirectory + "/" : "") + file.getName();

        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, fullPath, file);

        if (subdirectory.equals("data"))
            putObjectRequest.withTagging(new ObjectTagging(dataFileTag));

        client.putObject(putObjectRequest);
    }
}
