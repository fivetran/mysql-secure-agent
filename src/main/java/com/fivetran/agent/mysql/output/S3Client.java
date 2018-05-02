/**
* Copyright (c) Fivetran 2018
**/
package com.fivetran.agent.mysql.output;

import com.amazonaws.services.s3.AmazonS3Client;

import java.io.File;

public class S3Client implements BucketClient {
    private final String bucketName;
    private AmazonS3Client client;
    public S3Client(String bucketName) {
        this.bucketName = bucketName;
        this.client = new AmazonS3Client();
    }

    @Override
    public void copy(String prefix, File file) {
        client.putObject(bucketName, (!prefix.isEmpty()? prefix + "/" : "") + file.getName(), file);
    }
}
