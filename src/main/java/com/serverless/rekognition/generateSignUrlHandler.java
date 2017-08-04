package com.serverless.rekognition;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.serverless.rekognition.config.Config;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.File;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class generateSignUrlHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(generateSignUrlHandler.class);
    private String prefixUrl = "https://s3-eu-west-1.amazonaws.com/";

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("received: " + input.size() + " " + input.toString());
        String filetype = (String)input.get("content-type");
        LOG.info("fileType : "+filetype);
        String bucket_name = System.getenv(Config.BUCKET_NAME);
        if (bucket_name == null) {
            bucket_name = "";
        }
        LOG.info("Bucket Name : " + bucket_name);
        prefixUrl.concat(bucket_name + "/");

//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
//		AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(DefaultAWSCredentialsProviderChain.getInstance()).build();
        java.util.Date expiration = new java.util.Date();
        long msec = expiration.getTime();
        msec += 1000 * 60 * 60; // 1 hour.
        expiration.setTime(msec);

        LOG.info("Exp : "+expiration.toString());

        LocalDate date = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String text = date.format(formatter);
        LocalDate parsedDate = LocalDate.parse(text, formatter);


        String keyName = parsedDate + File.separator + System.currentTimeMillis();
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucket_name, keyName);
        generatePresignedUrlRequest.setMethod(HttpMethod.PUT);
        generatePresignedUrlRequest.setContentType("image/png");
        // setting http request header:
        // x-amx-canned-acl: 'public-read'
        generatePresignedUrlRequest.addRequestParameter(
                Headers.S3_CANNED_ACL,
                CannedAccessControlList.PublicRead.toString()
        );
        //Access-Control-Allow-Origin
//        generatePresignedUrlRequest.addRequestParameter(
//                "Access-Control-Allow-Origin",
//                "*"
//        );
        generatePresignedUrlRequest.setExpiration(expiration);
        s3client.putObject(bucket_name,keyName,"temp");
        URL signUrl = s3client.generatePresignedUrl(generatePresignedUrlRequest);
        String signUrlString = signUrl.toString();

        Map<String, Object> output = new HashMap<String, Object>();
        output.put("signUrl", signUrlString);
        output.put("url", prefixUrl + keyName);
        Response response = new Response("generateURL", output);

//        String body = "signUrl : "+signUrlString+","+"url : " +prefixUrl+keyName;

        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(response)
                .setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
                .build();
    }


}
