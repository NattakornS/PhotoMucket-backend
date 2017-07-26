package com.serverless.rekognition;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.SearchFacesByImageRequest;
import com.amazonaws.services.rekognition.model.SearchFacesByImageResult;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.serverless.rekognition.config.Config;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class getUserDataHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(getUserDataHandler.class);
    private Float threshold = 70F;
    private int maxFaces = 2;

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("received: " + input);
        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);

        AmazonRekognition amazonRekognition = AmazonRekognitionClientBuilder.standard().build();
        String bucket = (String)input.get("bucket");
        String key = (String)input.get("key");
        Image source = getImageUtil(bucket,key);

        SearchFacesByImageResult searchFacesByImageResult = callSearchFacesByImage(Config.collectionId, source, threshold, maxFaces, amazonRekognition);
        LOG.info(searchFacesByImageResult.toString());
        Map<String, Object> output = new HashMap<>();
        output.put("faceMatch", searchFacesByImageResult.toString());
        Response response = new Response("Face Search result", output);

        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(response)
                .setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
                .build();
    }

    private static SearchFacesByImageResult callSearchFacesByImage(String collectionId,
                                                                   Image image, Float threshold, int maxFaces, AmazonRekognition amazonRekognition
    ) {
        SearchFacesByImageRequest searchFacesByImageRequest = new SearchFacesByImageRequest()
                .withCollectionId(collectionId)
                .withImage(image)
                .withFaceMatchThreshold(threshold)
                .withMaxFaces(maxFaces);
        return amazonRekognition.searchFacesByImage(searchFacesByImageRequest);
    }

    private static Image getImageUtil(String bucket, String key) {
        return new Image()
                .withS3Object(new S3Object()
                        .withBucket(bucket)
                        .withName(key));
    }

//		s3client.
//        try {
//            System.out.println("Uploading a new object to S3 from a file\n");
//            File file = new File(uploadFileName);
//            s3client.putObject(new PutObjectRequest(
//                    bucketName, keyName, file));
//
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught an AmazonServiceException, which " +
//                    "means your request made it " +
//                    "to Amazon S3, but was rejected with an error response" +
//                    " for some reason.");
//            System.out.println("Error Message:    " + ase.getMessage());
//            System.out.println("HTTP Status Code: " + ase.getStatusCode());
//            System.out.println("AWS Error Code:   " + ase.getErrorCode());
//            System.out.println("Error Type:       " + ase.getErrorType());
//            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
//            System.out.println("Caught an AmazonClientException, which " +
//                    "means the client encountered " +
//                    "an internal error while trying to " +
//                    "communicate with S3, " +
//                    "such as not being able to access the network.");
//            System.out.println("Error Message: " + ace.getMessage());
//        }
//}
}
