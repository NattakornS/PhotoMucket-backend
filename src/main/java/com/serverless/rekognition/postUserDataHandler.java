package com.serverless.rekognition;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.serverless.rekognition.config.ApiParameter;
import com.serverless.rekognition.config.Config;
import com.serverless.rekognition.config.TableHeader;
import org.apache.log4j.Logger;

import java.util.*;

public class postUserDataHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(postUserDataHandler.class);

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("received: " + input);
        String collectionId = System.getenv(Config.REKOGNITION_COLLECTION_NAME);
        LOG.info("CollectionID : " + collectionId);
//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);

//		AWSCredentials credentials;
//		try {
//			credentials = new ProfileCredentialsProvider("default").getCredentials();
//		} catch (Exception e) {
//			throw new AmazonClientException(
//					"Cannot load the credentials from the credential profiles file. " +
//							"Please make sure that your credentials file is at the correct " +
//							"location (/Users/userid/.aws/credentials), and is in valid format.",
//					e);
//		}

//        Object body = input.get("body");
//
        if (input == null) {
            HashMap<String, Object> output = new HashMap<>();
            output.put("error", "invalid body");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(Collections.singletonMap("Rekognition", "Rekognition - user data post"))
                    .build();
        }
//
//
//        HashMap<String,String> bodyMap = (HashMap<String,String>) body;

        String key = input.get(ApiParameter.PostUserData.KEY) != null ? input.get(ApiParameter.PostUserData.KEY).toString() : "";
        String birthday = input.get(ApiParameter.PostUserData.BIRTHDAY) != null ? input.get(ApiParameter.PostUserData.BIRTHDAY).toString() : "";
        String bucket = input.get(ApiParameter.PostUserData.BUCKET) != null ? input.get(ApiParameter.PostUserData.BUCKET).toString() : "";
        String description = input.get(ApiParameter.PostUserData.DESCRIPTION) != null ? input.get(ApiParameter.PostUserData.DESCRIPTION).toString() : "";
        String email = input.get(ApiParameter.PostUserData.EMAIL) != null ? input.get(ApiParameter.PostUserData.EMAIL).toString() : "";
        String firstame = input.get(ApiParameter.PostUserData.FIRSTNAME) != null ? input.get(ApiParameter.PostUserData.FIRSTNAME).toString() : "";
        String surename = input.get(ApiParameter.PostUserData.SURENAME) != null ? input.get(ApiParameter.PostUserData.SURENAME).toString() : "";
        String nickname = input.get(ApiParameter.PostUserData.NICKNAME) != null ? input.get(ApiParameter.PostUserData.NICKNAME).toString() : "";
        String phone = input.get(ApiParameter.PostUserData.PHONE) != null ? input.get(ApiParameter.PostUserData.PHONE).toString() : "";
        String imageUrl = input.get(ApiParameter.PostUserData.IMAGEURL) != null ? input.get(ApiParameter.PostUserData.IMAGEURL).toString() : "";

        if (key == "" || bucket == "" || email == "" || firstame == "") {
            HashMap<String, Object> output = new HashMap<>();
            output.put("error", "invalid body");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(Collections.singletonMap("Rekognition", "Rekognition - user data post"))
                    .build();
        }
        AmazonRekognition amazonRekognition = AmazonRekognitionClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();

        AmazonDynamoDB amazonDynamoDB = AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();


        if (!isCollectionExist(collectionId, amazonRekognition)) {

            CreateCollectionResult createCollectionResult = callCreateCollection(collectionId, amazonRekognition);
            LOG.info(createCollectionResult.toString());
        }

//        String keyName = (String) input.get("key");

//        String bucket_name = System.getenv(Config.BUCKET_NAME);
        String user_table_name = System.getenv(Config.USER_DYNAMODB_TABLE);

        LOG.info("Bucket Name : " + bucket + ", Key : " + key);
        Image image = getImageUtil(bucket, key);
        String externalImageId = key;
        IndexFacesResult indexFacesResult = callIndexFaces(collectionId,
                externalImageId, "ALL", image, amazonRekognition);
        System.out.println(externalImageId + " added");
        List<FaceRecord> faceRecords = indexFacesResult.getFaceRecords();


        if (faceRecords.size() <= 0) {
            HashMap<String, Object> output = new HashMap<>();
            output.put("error", "no face detect");
            output.put("description", "Please insert face image.");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(Collections.singletonMap("Rekognition", "Rekognition - user data post"))
                    .build();
        }

        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);

        Table table = dynamoDB.getTable(user_table_name);

//        String sureName = "";
//        String name = "";
//        String nickName = "";
//        String email = "";
//        String faceId = "";
//        String imageId = "";
//        String imageUrl = "";
//        String phone = "";


        String faceDetectTxt = "";

        for (FaceRecord faceRecord : faceRecords) {
            int itemAcumulate=0;
            String faceId = faceRecord.getFace().getFaceId();
            String imageId = faceRecord.getFace().getImageId();
            faceDetectTxt += "Face detected: Faceid is " +
                    faceId + "/n";


//            ScanFilter spec = new ScanFilter(TableHeader.FACE_ID).eq("faceId");

            ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#faceid,email")
                    .withFilterExpression("#faceid = :inputfaceid").withNameMap(new NameMap().with("#faceid", "faceid"))
                    .withValueMap(new ValueMap().withString(":inputfaceid",faceId));

            try {
                ItemCollection<ScanOutcome> items = table.scan(scanSpec);
                itemAcumulate = items.getAccumulatedItemCount();
                Iterator<Item> iter = items.iterator();

                while (iter.hasNext()) {
                    itemAcumulate++;
                    Item item = iter.next();
                    LOG.info("face dup : " +item.toString());
                }
            }
            catch (Exception e) {
                System.err.println("Unable to scan the table:");
                System.err.println(e.getMessage());
            }

            LOG.info("ItemAcumulate : "+ itemAcumulate);
//            if (itemAcumulate <= 0) {
                Item item = new Item()
                        .withPrimaryKey(TableHeader.NAME, firstame)
                        .withString(TableHeader.SURE_NAME, surename)
                        .withString(TableHeader.NICK_NAME, nickname)
                        .withString(TableHeader.PHONE, birthday)
                        .withString(TableHeader.PHONE, description)
                        .withString(TableHeader.EMAIL, email)
                        .withString(TableHeader.FACE_ID, faceId)
                        .withString(TableHeader.IMAGE_ID, imageId)
                        .withString(TableHeader.IMAGE_URL, imageUrl)
                        .withString(TableHeader.PHONE, phone);
                table.putItem(item);
                LOG.info("Add user data to table : " + item.toString());
//            }else{
//                LOG.info("Duplicate Face");
//            }
        }
        System.out.println(faceDetectTxt);
        input.put("FaceDetect", faceDetectTxt);
        Response response = new Response(Config.ResponeseKey, input);
        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(response)
                .setHeaders(Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless"))
                .build();
    }

    private static CreateCollectionResult callCreateCollection(String collectionId,
                                                               AmazonRekognition amazonRekognition) {
        CreateCollectionRequest request = new CreateCollectionRequest()
                .withCollectionId(collectionId);
        return amazonRekognition.createCollection(request);
    }

    private static ListCollectionsResult callListCollections(String paginationToken,
                                                             int limit, AmazonRekognition amazonRekognition) {
        ListCollectionsRequest listCollectionsRequest = new ListCollectionsRequest()
                .withMaxResults(limit)
                .withNextToken(paginationToken);
        return amazonRekognition.listCollections(listCollectionsRequest);
    }

    private boolean isCollectionExist(String collectionId, AmazonRekognition amazonRekognition) {
        int limit = 1;
        String paginationToken = null;
        ListCollectionsResult listCollectionsResult = null;
        do {
            if (listCollectionsResult != null) {
                paginationToken = listCollectionsResult.getNextToken();
            }
            listCollectionsResult = callListCollections(paginationToken, limit,
                    amazonRekognition);

            List<String> collectionIds = listCollectionsResult.getCollectionIds();
            for (String resultId : collectionIds) {
                LOG.info(resultId);
                if (resultId.equals(collectionId)) {
                    LOG.info("collection already exist");
                    return true;
                }
            }
        } while (listCollectionsResult != null && listCollectionsResult.getNextToken() !=
                null);
        return false;
    }

    private static Image getImageUtil(String bucket, String key) {
        return new Image()
                .withS3Object(new S3Object()
                        .withBucket(bucket)
                        .withName(key));
    }

    private static IndexFacesResult callIndexFaces(String collectionId, String externalImageId,
                                                   String attributes, Image image, AmazonRekognition amazonRekognition) {
        IndexFacesRequest indexFacesRequest = new IndexFacesRequest()
                .withImage(image)
                .withCollectionId(collectionId)
//                .withExternalImageId(externalImageId)
                .withDetectionAttributes(attributes);
        return amazonRekognition.indexFaces(indexFacesRequest);

    }

    private static ListFacesResult callListFaces(String collectionId, int limit,
                                                 String paginationToken, AmazonRekognition amazonRekognition) {
        ListFacesRequest listFacesRequest = new ListFacesRequest()
                .withCollectionId(collectionId)
                .withMaxResults(limit)
                .withNextToken(paginationToken);
        return amazonRekognition.listFaces(listFacesRequest);
    }
}
