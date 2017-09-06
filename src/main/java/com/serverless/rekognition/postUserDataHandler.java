package com.serverless.rekognition;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.serverless.rekognition.config.ApiParameter;
import com.serverless.rekognition.config.Config;
import com.serverless.rekognition.config.TableHeader;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class postUserDataHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(postUserDataHandler.class);

    private Map<String, String> defaultHeader = Collections.singletonMap("Rekognition", "Rekognition - user data post");
    private String key;
    private String fileName;
    private String birthday;
    private String bucket;
    private String description;
    private String email;
    private String firstname;
    private String surename;
    private String nickname;
    private String phone;
    private String imageUrl;


    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {

        String collection_Id = System.getenv(Config.REKOGNITION_COLLECTION_NAME);
        String user_table_name = System.getenv(Config.USER_DYNAMODB_TABLE);
        String rek_table_name = System.getenv(Config.REK_DYNAMODB_TABLE);
        LOG.info("received: " + input);


        if (input == null) {
            HashMap<String, Object> output = new HashMap<>();
            output.put("error", "invalid body");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(defaultHeader)
                    .build();
        }

        boolean getInputBool = getInputData(input);

        if (!getInputBool){
            HashMap<String, Object> output = new HashMap<>();
            output.put("error", "invalid body");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(defaultHeader)
                    .build();
        }


        //Initial Services
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


        if (!isCollectionExist(collection_Id, amazonRekognition)) {

            CreateCollectionResult createCollectionResult = callCreateCollection(collection_Id, amazonRekognition);
            LOG.info(createCollectionResult.toString());
        }


        //Indexface
        LOG.info("IndexFace to Collection : "+collection_Id);
        LOG.info("Bucket Name : " + bucket + ", Key : " + key);
        Image image = getImageUtil(bucket, key);
        IndexFacesResult indexFacesResult = callIndexFaces(collection_Id,
                fileName, "ALL", image, amazonRekognition);
        System.out.println(fileName + " added");
        List<FaceRecord> faceRecords = indexFacesResult.getFaceRecords();


        if (faceRecords.size() <= 0) {
            HashMap<String, Object> output = new HashMap<>();
            output.put(ApiParameter.errorResponse.ERROR, "no face detect");
            output.put(ApiParameter.errorResponse.DESC, "Please insert face image.");
            Response response = new Response(Config.ResponeseKey, output);
            return ApiGatewayResponse.builder()
                    .setStatusCode(400)
                    .setObjectBody(response)
                    .setHeaders(defaultHeader)
                    .build();
        }

        //Initial table
        LOG.info("Initial Table");
        DynamoDB dynamoDB = new DynamoDB(amazonDynamoDB);
        Table userTable = dynamoDB.getTable(user_table_name);
        Table rekTable = dynamoDB.getTable(rek_table_name);

        //AddUser to Table
        LOG.info("Add User to : "+user_table_name);
        Item userItem = new Item()
                .withPrimaryKey(TableHeader.EMAIL, email)
                .withString(TableHeader.NAME, firstname)
                .withString(TableHeader.SURE_NAME, surename)
                .withString(TableHeader.NICK_NAME, nickname)
                .withString(TableHeader.PHONE, birthday)
                .withString(TableHeader.DESC, description)
                .withString(TableHeader.IMAGE_URL, imageUrl)
                .withString(TableHeader.PHONE, phone);
        userTable.putItem(userItem);


//        String faceDetectTxt = "";

        // Add FaceId to Rek Table
        LOG.info("Add FaceId to : " + rek_table_name);
        for (FaceRecord faceRecord : faceRecords) {
//            int itemAcumulate = 0;
            String faceId = faceRecord.getFace().getFaceId();
            String imageId = faceRecord.getFace().getImageId();
//            String externalImageId = faceRecord.getFace().getExternalImageId();
//            faceDetectTxt += "Face detected: Faceid is " +
//                    faceId + "/n";

            // filter if already have faceId in Table Should not replace or update.
//            ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#faceid,email")
//                    .withFilterExpression("#faceid = :inputfaceid").withNameMap(new NameMap().with("#faceid", "faceid"))
//                    .withValueMap(new ValueMap().withString(":inputfaceid", faceId));
//            try {
//                ItemCollection<ScanOutcome> items = rekTable.scan(scanSpec);
//                itemAcumulate = items.getAccumulatedItemCount();
//                Iterator<Item> iter = items.iterator();
//
//                while (iter.hasNext()) {
//                    itemAcumulate++;
//                    Item item = iter.next();
//                    LOG.info("face dup : " + item.toString());
//                }
//            } catch (Exception e) {
//                LOG.error("Unable to scan the userTable : "+e.getMessage());
//            }
//
//            LOG.info("ItemAcumulate : " + itemAcumulate);
//            if (itemAcumulate <= 0) {
                Item rekItem = new Item()
                        .withPrimaryKey(TableHeader.FACE_ID, faceId)
                        .withString(TableHeader.EMAIL, email)
                        .withString(TableHeader.IMAGE_ID, imageId);
    //                    .withString(TableHeader.EXTERNAL_IMAGE_ID,externalImageId)
    //                    .withString(TableHeader.IMAGE_URL, imageUrl);
                rekTable.putItem(rekItem);
                LOG.info("Add user data to userTable : " + rekItem.toString());
//            }else{
//                LOG.info("Duplicate Face");
//            }
        }
        LOG.info(faceRecords.toString());
        input.put("FaceDetected", faceRecords.size());
        input.put("FaceDetectedDetail",faceRecords);
        input.put("Success",true);
        Response response = new Response(Config.ResponeseKey, input);
        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(response)
                .setHeaders(defaultHeader)
                .build();
    }

    private boolean getInputData(Map<String, Object> input) {
        key = input.get(ApiParameter.PostUserData.KEY) != null ? input.get(ApiParameter.PostUserData.KEY).toString() : "";
        fileName = input.get(ApiParameter.PostUserData.FILE_NAME) != null ? input.get(ApiParameter.PostUserData.FILE_NAME).toString() : "";
        birthday = input.get(ApiParameter.PostUserData.BIRTHDAY) != null ? input.get(ApiParameter.PostUserData.BIRTHDAY).toString() : "";
        bucket = input.get(ApiParameter.PostUserData.BUCKET) != null ? input.get(ApiParameter.PostUserData.BUCKET).toString() : "";
        description = input.get(ApiParameter.PostUserData.DESCRIPTION) != null ? input.get(ApiParameter.PostUserData.DESCRIPTION).toString() : "";
        email = input.get(ApiParameter.PostUserData.EMAIL) != null ? input.get(ApiParameter.PostUserData.EMAIL).toString() : "";
        firstname = input.get(ApiParameter.PostUserData.FIRSTNAME) != null ? input.get(ApiParameter.PostUserData.FIRSTNAME).toString() : "";
        surename = input.get(ApiParameter.PostUserData.SURENAME) != null ? input.get(ApiParameter.PostUserData.SURENAME).toString() : "";
        nickname = input.get(ApiParameter.PostUserData.NICKNAME) != null ? input.get(ApiParameter.PostUserData.NICKNAME).toString() : "";
        phone = input.get(ApiParameter.PostUserData.PHONE) != null ? input.get(ApiParameter.PostUserData.PHONE).toString() : "";
        imageUrl = input.get(ApiParameter.PostUserData.IMAGEURL) != null ? input.get(ApiParameter.PostUserData.IMAGEURL).toString() : "";

        if (key == "" || bucket == "" || email == "" || firstname == "") {
           return false;
        }
        return true;
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

    private Image getImageUtil(String bucket, String key) {
        return new Image()
                .withS3Object(new S3Object()
                        .withBucket(bucket)
                        .withName(key));
    }

    private IndexFacesResult callIndexFaces(String collectionId, String externalImageId,
                                                   String attributes, Image image, AmazonRekognition amazonRekognition) {
        IndexFacesRequest indexFacesRequest = new IndexFacesRequest()
                .withImage(image)
                .withCollectionId(collectionId)
                .withExternalImageId(externalImageId)
                .withDetectionAttributes(attributes);
        return amazonRekognition.indexFaces(indexFacesRequest);

    }

    private ListFacesResult callListFaces(String collectionId, int limit,
                                                 String paginationToken, AmazonRekognition amazonRekognition) {
        ListFacesRequest listFacesRequest = new ListFacesRequest()
                .withCollectionId(collectionId)
                .withMaxResults(limit)
                .withNextToken(paginationToken);
        return amazonRekognition.listFaces(listFacesRequest);
    }
}
