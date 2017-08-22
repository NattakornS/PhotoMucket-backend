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

        Map<String, Object> output_return = new HashMap<>();
        //AmazonDynamoDB Config
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        List<String> detail_user_json = new ArrayList<String>();
        List<String> check_email = new ArrayList<String>();

        //get Table for Scan
        String table_rek =System.getenv(Config.REK_DYNAMODB_TABLE);
        Table table = dynamoDB.getTable(table_rek);
        String table_user =System.getenv(Config.USER_DYNAMODB_TABLE);
        Table table_register = dynamoDB.getTable(table_user);

        //get Email From Table Rek
        for(int i =0 ;i<face_id.size();i++)
        {
            ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#faceid, email")
                    .withFilterExpression("#faceid = :inputfaceid").withNameMap(new NameMap().with("#faceid", "faceid"))
                    .withValueMap(new ValueMap().withString(":inputfaceid",face_id.get(i).toString()));
            try {
                ItemCollection<ScanOutcome> items = table.scan(scanSpec);
                Iterator<Item> iter = items.iterator();
                while (iter.hasNext()) {
                    Item item = iter.next();
                    //add email to List String for Chack
                    check_email.add(item.getString("email"));
                    LOG.info("Add email to Check list :"+item.getString("email"));
                }

            } catch (Exception e) {
                System.err.println("Unable to scan the table rekognition:"+e.getMessage());
                LOG.error("Unable to scan the table rekognition:"+e.getMessage());
            }

        }

        //check unique Email
        List<String> uniqueList = new ArrayList<String>(new HashSet<String>( check_email ));
        LOG.info("unique List Email size"+uniqueList.size());
        LOG.info("unique List Email object"+uniqueList.toString());

        for(int j =0 ;j<uniqueList.size();j++)
        {
            LOG.info("Email input to Scan"+uniqueList.get(j));

            ScanSpec scanSpec_register = new ScanSpec().withProjectionExpression("#email,nickname,description,imageurl,phone,surename,firstname")
                        .withFilterExpression("#email = :inputemail").withNameMap(new NameMap().with("#email", "email"))
                        .withValueMap(new ValueMap().withString(":inputemail",uniqueList.get(j)));

            try {
                ItemCollection<ScanOutcome> itemsuser = table_register.scan(scanSpec_register);
                Iterator<Item> iter = itemsuser.iterator();
                while (iter.hasNext()) {
                    Item item = iter.next();
                    LOG.info("Profile Return:"+item.toString());
                    detail_user_json.add(item.toJSON());
                }

            }catch (Exception e)
            {
                System.err.println("Unable to scan the table user:"+e.getMessage());
                LOG.error("Unable to scan the table user:"+e.getMessage());
            }

        }
        output_return.put("data",detail_user_json);
        return (HashMap) output_return;
    }

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {

        //Return
        List<String> ExternalImageId = new ArrayList<String>();
        String S3_URL ="https://s3.amazonaws.com";

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
        LOG.info("Check_parameter_input=>: imageUrl:" + imageUrl +"bucket:"+bucket+" Key :"+ key);



        List<String> face_id = new ArrayList<String>();
        Response response;
        Map<String, Object> output_response = new HashMap<>();

        HashMap<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin","*");
        headers.put("Access-Control-Allow-Credentials","true");
        headers.put("Access-Control-Allow-Methods","GET,PUT,POST,DELETE");
        String bucketname =System.getenv(Config.BUCKET_NAME);
        try {
            AmazonRekognition amazonRekognition = AmazonRekognitionClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
            Image source = getImageUtil(bucket, key);
            SearchFacesByImageResult searchFacesByImageResult = callSearchFacesByImage(collectionId, source, threshold, maxFaces, amazonRekognition);
            if(searchFacesByImageResult.getFaceMatches().size() != 0)
            {
                LOG.info("faceMatch:"+searchFacesByImageResult.getFaceMatches());
                for(int i=0 ;i<searchFacesByImageResult.getFaceMatches().size();i++)
                {
                face_id.add(searchFacesByImageResult.getFaceMatches().get(i).getFace().getFaceId().toString());
                ExternalImageId.add(S3_URL+"/"+bucketname+"/"+searchFacesByImageResult.getFaceMatches().get(i).getFace().getExternalImageId());
                LOG.info("Add FaceID to Array:"+searchFacesByImageResult.getFaceMatches().get(i).getFace().getFaceId().toString());
                }

            }else{
                LOG.info("Not Found FaceMatch :");
            }

            Map<String, Object> output2 =getUrl_faceMatches(face_id);
            output_response =output2;
            output_response.put("profile",ExternalImageId);

        }catch (Exception error)
        {
            LOG.error("Error message :"+error);
           response = new Response("Error", null);
        }
        return ApiGatewayResponse.builder()
                .setStatusCode(200)
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
