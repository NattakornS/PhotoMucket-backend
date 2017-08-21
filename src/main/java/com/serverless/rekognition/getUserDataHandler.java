package com.serverless.rekognition;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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
import com.serverless.rekognition.config.ApiParameter;
import com.serverless.rekognition.config.Config;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

public class getUserDataHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(getUserDataHandler.class);
    private Float threshold = 70F;
    private int maxFaces = 2;

    public HashMap  getUrl_faceMatches(List<String> face_id)
    {
        //connect DynamoDB
        Map<String, Object> output_return = new HashMap<>();
        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        List<String> face_detail_json = new ArrayList<String>();
        Table table = dynamoDB.getTable("rekognition-demo-rek-dev");

        for(int i =0 ;i<face_id.size();i++)
        {
            ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#faceid, email,imageurl")
                    .withFilterExpression("#faceid = :inputfaceid").withNameMap(new NameMap().with("#faceid", "faceid"))
                    .withValueMap(new ValueMap().withString(":inputfaceid",face_id.get(i).toString()));


            try {
                ItemCollection<ScanOutcome> items = table.scan(scanSpec);

                Iterator<Item> iter = items.iterator();
                while (iter.hasNext()) {
                    Item item = iter.next();

                    LOG.info("item:"+item.toString());
                    LOG.info("get in item : faceid: "+item.getJSON("faceid")+"imageurl:"+item.getJSON("imageurl"));
                    face_detail_json.add(item.toJSON());
                }

            } catch (Exception e) {
                System.err.println("Unable to scan the table:");
                System.err.println(e.getMessage());
            }

        }

        output_return.put("data",face_detail_json);

        return (HashMap) output_return;
    }

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        String collectionId = System.getenv(Config.REKOGNITION_COLLECTION_NAME);
//        input parameter
//        {
//            "imageUrl":"http://s3.amazonaws.com/Rekognitiondemo/15082017/imagename.jpg",
//                "bucket":"Rekognitiondemo",
//                "key":"15082017/imagename.jpg"
//        }

//      getparameter
        String imageUrl = input.get(ApiParameter.getUserDataHandler.IMAGEURL).toString();
        String bucket = input.get(ApiParameter.getUserDataHandler.BUCKET).toString();
        String key = input.get(ApiParameter.getUserDataHandler.KEY).toString();
        LOG.info("check_parameter_input: imageUrl:" + imageUrl +"bucket:"+bucket+" Key :"+ key);



        List<String> face_id = new ArrayList<String>();

        Response response;
        Map<String, Object> output_response = new HashMap<>();

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin","*");
        headers.put("Access-Control-Allow-Credentials","true");
        headers.put("Access-Control-Allow-Methods","GET,PUT,POST,DELETE");


//        LOG.info("received: " + input);
//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);

        try {
            LOG.info("In try :");
            AmazonRekognition amazonRekognition = AmazonRekognitionClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
            LOG.info("Image source :");
            Image source = getImageUtil(bucket, key);
            LOG.info("Before searchFacesByImageResult :");
            SearchFacesByImageResult searchFacesByImageResult = callSearchFacesByImage(collectionId, source, threshold, maxFaces, amazonRekognition);
            if(searchFacesByImageResult.getFaceMatches().size() != 0)
            {
                for(int i=0 ;i<searchFacesByImageResult.getFaceMatches().size();i++)
                {

                face_id.add(searchFacesByImageResult.getFaceMatches().get(i).getFace().getFaceId().toString());
                LOG.info("push to Array FaceID:"+searchFacesByImageResult.getFaceMatches().get(i).getFace().getFaceId().toString());
                }

            }else{
                LOG.info("Not Found FaceMatch :");
            }

//            LOG.info("searchFacesByImageResult: "+searchFacesByImageResult.toString());
//            LOG.info("searchFacesByImageResult.getFaceMatches:"+searchFacesByImageResult.getFaceMatches().toString());
//            LOG.info("searchFacesByImageResult.getFaceMatches.size:"+searchFacesByImageResult.getFaceMatches().size());
//            LOG.info("searchFacesByImageResult.getFaceMatches().get(1).getFace().getFaceId():"+searchFacesByImageResult.getFaceMatches().get(1).getFace().getFaceId().toString());
//            LOG.info("searchFacesByImageResult.getSearchedFaceBoundingBox"+searchFacesByImageResult.getSearchedFaceBoundingBox().toString());
//            LOG.info("searchFacesByImageResult.getSearchedFaceConfidence"+searchFacesByImageResult.getSearchedFaceConfidence().toString());
//            LOG.info("searchFacesByImageResult.getFaceMatches size:"+searchFacesByImageResult.getFaceMatches().size());
//            LOG.info("searchFacesByImageResult.getSdkHttpMetadata:"+searchFacesByImageResult.getSdkHttpMetadata().getHttpHeaders().toString());
//            LOG.info("searchFacesByImageResult.getSdkResponseMetadata"+searchFacesByImageResult.getSdkResponseMetadata().toString());

            Map<String, Object> output2 =getUrl_faceMatches(face_id);
            output_response =output2;
            Map<String, Object> output = new HashMap<>();
            LOG.info("output:");
            output.put("faceMatch", searchFacesByImageResult.toString());




//            response = new Response("Face Search result", output2);

        }catch (Exception error)
        {
            LOG.error("Error message :"+error);
           response = new Response("Error", null);
        }
        return ApiGatewayResponse.builder()
                .setStatusCode(200)
//                .setObjectBody(response)
                .setObjectBody(output_response)
                .setHeaders(headers)
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

}
