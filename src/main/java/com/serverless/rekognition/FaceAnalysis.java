package com.serverless.rekognition;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.serverless.rekognition.config.ApiParameter;
import com.sun.media.jfxmedia.logging.Logger;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FaceAnalysis implements RequestHandler<Map<String, Object> , ApiGatewayResponse> {
    private static final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(FaceAnalysis.class);
    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {

//        String photo = "photo.jpg";
//        String bucket = "S3bucket";

        String bucket = input.get(ApiParameter.getUserDataHandler.BUCKET).toString();
        String key = input.get(ApiParameter.getUserDataHandler.KEY).toString();
        Map<String, Object> output_response = new HashMap<>();
        HashMap<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin","*");
        headers.put("Access-Control-Allow-Credentials","true");
        headers.put("Access-Control-Allow-Methods","GET,PUT,POST,DELETE");

        AmazonRekognition rekognitionClient  = AmazonRekognitionClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        Image source = getImageUtil(bucket, key);
        DetectFacesRequest request = new DetectFacesRequest()
                .withImage(source)
                .withAttributes(Attribute.ALL);

        try {
            DetectFacesResult result = rekognitionClient.detectFaces(request);
            List< FaceDetail > faceDetails = result.getFaceDetails();
            LOG.info(faceDetails.toString());

            for (FaceDetail face: faceDetails) {
                if (request.getAttributes().contains("ALL")) {
                    AgeRange ageRange = face.getAgeRange();
                    String facre =face.toString();
                    System.out.println("facre:"+facre);
                    output_response.put("detailface",face);

                    System.out.println("The detected face is estimated to be between "
                            + ageRange.getLow().toString() + " and " + ageRange.getHigh().toString()
                            + " years old.");


                    String Emotions = face.getEmotions().toString();
                    System.out.println("emogination:"+Emotions);

                    String Gender = face.getGender().toString();
                    System.out.println("Gender:"+Gender);

                    String Smile = face.getSmile().toString();
                    System.out.println("Smile:"+Smile);

                    String Sunglasses = face.getSunglasses().toString();
                    System.out.println("Sunglasses:"+Sunglasses);

                    System.out.println("Here's the complete set of attributes:");
                } else { // non-default attributes have null values.
                    System.out.println("Here's the default set of attributes:");
                }

                ObjectMapper objectMapper = new ObjectMapper();

//                System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(face));
            }

        } catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }


        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(output_response)
                .setHeaders(headers)
                .build();
    }

    private static Image getImageUtil(String bucket, String key) {
        return new Image()
                .withS3Object(new com.amazonaws.services.rekognition.model.S3Object()
                        .withBucket(bucket)
                        .withName(key));
    }

}
