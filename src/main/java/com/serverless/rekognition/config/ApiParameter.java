package com.serverless.rekognition.config;

public class ApiParameter {

    //getSignUrl
    public interface GetSignURL{
        String FILE_TYPE = "fileType";
        String FILE_NAME = "fileName";
    }

    //response SignUrl
    public interface ResponseSignURL {
        String SIGN_URL = "signUrl";
        String IMAGE_URL = "imageUrl";
        String KEY_NAME = "key";
        String BUCKET_NAME = "bucket";
    }
}
