package com.serverless.rekognition;

import com.amazonaws.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.*;
import com.serverless.rekognition.config.Config;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class generateSignUrlHandler implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = Logger.getLogger(generateSignUrlHandler.class);
    private String prefixUrl = "https://s3.amazonaws.com/";
    private Map<String, String> headers = null;

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input, Context context) {
        LOG.info("received: " + input.size() + " " + input.toString());
        Response response = null;
        String fileType = "image/png";
        String fileName = "filename";
        Object queryStringParameters = input.get("queryStringParameters");
        LOG.info("qeuryString : " + queryStringParameters.toString());

        if (queryStringParameters != null) {
            Map<String, String> params = (Map<String, String>) queryStringParameters;
            fileName = params.get("filename");
            fileType = params.get("filetype");
            LOG.info("file name : " + fileName + "\n" + "file type : " + fileType);
        }else{
            headers.put("error","missing file name and type");
            return ApiGatewayResponse.builder()
                    .setStatusCode(200)
                    .setObjectBody(response)
                    .setHeaders(headers)
                    .build();
        }

        /*if (params!=null){
            try {
                String filetype = "";
                String filename = "";
                JsonFactory factory = new JsonFactory();
                JsonParser  parser  = factory.createParser(params.toString());
                while(!parser.isClosed()){
                    JsonToken jsonToken = parser.nextToken();

                    if(JsonToken.FIELD_NAME.equals(jsonToken)){
                        String fieldName = parser.getCurrentName();
                        System.out.println(fieldName);

                        jsonToken = parser.nextToken();

                        if("filename".equals(fieldName)){
                            filename = parser.getValueAsString();
                        } else if ("filetype".equals(fieldName)){
                            filetype = parser.getValueAsString();
                        }
                    }
                }

                LOG.info("file name : "+filename+"\n"+"file type : "+filetype);
            } catch (IOException e) {
               LOG.error(e.getMessage());
            }
        }*/
        String bucket_name = System.getenv(Config.BUCKET_NAME);
        if (bucket_name == null) {
            bucket_name = "";
        }
        LOG.info("Bucket Name : " + bucket_name);
        prefixUrl = prefixUrl.concat(bucket_name + "/");

//        Response responseBody = new Response("Go Serverless v1.x! Your function executed successfully!", input);
//		AmazonS3 s3Client = new AmazonS3ClientBuilder.with(new ProfileCredentialsProvider());
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(true)
                .build();
        try {
            java.util.Date expiration = new java.util.Date();
            long msec = expiration.getTime();
            msec += 1000 * 60 * 60; // 1 hour.
            expiration.setTime(msec);

            LOG.info("Exp : " + expiration.toString());

            LocalDate date = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String text = date.format(formatter);
            LocalDate parsedDate = LocalDate.parse(text, formatter);
            String s3FileName = System.currentTimeMillis() + fileName;
            String keyName = parsedDate + "/" + s3FileName;


            // create temp file and add acl

//            Bucket bucket = s3client.createBucket(bucket_name);
/*            File file = new File(s3FileName);
            AccessControlList acl = new AccessControlList();
            acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
            s3client.putObject(new PutObjectRequest(bucket_name, keyName, file).withAccessControlList(acl));
            s3client.setObjectAcl(bucket_name, keyName, CannedAccessControlList.PublicRead);*/

            GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucket_name, keyName);
            generatePresignedUrlRequest.setMethod(HttpMethod.PUT);
            generatePresignedUrlRequest.setContentType(fileType);
            generatePresignedUrlRequest.setExpiration(expiration);
            // setting http request header:
            // x-amx-canned-acl: 'public-read'
//            generatePresignedUrlRequest.addRequestParameter(
//                    Headers.S3_CANNED_ACL,
//                    CannedAccessControlList.PublicRead.toString()
//            );
            //Access-Control-Allow-Origin
//            generatePresignedUrlRequest.addRequestParameter(
//                    "Access-Control-Allow-Origin",
//                    "*"
//            );


            URL signUrl = s3client.generatePresignedUrl(generatePresignedUrlRequest);
//            CannedAccessControlList acl = new CannedAccessControlList(CannedAccessControlList.PublicRead.toString());
//            s3client.setObjectAcl(bucket_name,keyName,acl);
            String signUrlString = signUrl.toURI().toString();

            LOG.info("signedUrl : " + signUrlString);
//            UploadObject(signUrl);
            Map<String, Object> output = new HashMap<String, Object>();
            output.put("signUrl", signUrlString);
            output.put("url", prefixUrl + keyName);
            response = new Response("generateURL", output);

//        String body = "signUrl : "+signUrlString+","+"url : " +prefixUrl+keyName;
            headers = new HashMap<>();//("X-Powered-By", "AWS Lambda & serverless");
            headers.put("X-Powered-By", "AWS Lambda & serverless");
            headers.put("Access-Control-Allow-Origin", "*");

        } catch (AmazonServiceException exception) {
            System.out.println("Caught an AmazonServiceException, " +
                    "which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            LOG.error("ErrorMessage: " + exception.getMessage());
            LOG.error("HTTPCode: " + exception.getStatusCode());
            LOG.error("AWSErrorCode:" + exception.getErrorCode());
            LOG.error("ErrorType:    " + exception.getErrorType());
            LOG.error("RequestID:    " + exception.getRequestId());

            Map<String, Object> output = new HashMap<String, Object>();
            output.put("Error Message", exception.getMessage());
            output.put("HTTP  Code", exception.getStatusCode());
            output.put("AWS Error Code:", exception.getMessage());
            output.put("Error Type", exception.getStatusCode());
            output.put("Request ID", exception.getMessage());
            headers = Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless");
            response = new Response("generateURL", output);

        } catch (AmazonClientException ace) {
            LOG.error("Caught an AmazonClientException, " +
                    "which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            LOG.error("Error Message: " + ace.getMessage());
            Map<String, Object> output = new HashMap<String, Object>();
            output.put("ErrorMessage", ace.getMessage());
            headers = Collections.singletonMap("X-Powered-By", "AWS Lambda & serverless");
            response = new Response("generateURL", output);
        } catch (URISyntaxException e) {
            LOG.error("Error Message: " + e.getMessage());
            Map<String, Object> output = new HashMap<String, Object>();
            output.put("ErrorMessage", e.getMessage());
            response = new Response("Error", output);
        }
//        catch (IOException e) {
//            LOG.error("Error Message: " + e.getMessage());
//            Map<String, Object> output = new HashMap<String, Object>();
//            output.put("ErrorMessage", e.getMessage());
//            response = new Response("generateURL", output);
//        }
        return ApiGatewayResponse.builder()
                .setStatusCode(200)
                .setObjectBody(response)
                .setHeaders(headers)
                .build();
    }

    private void UploadObject(URL url) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(
                connection.getOutputStream());
        out.write("This text uploaded as object.");
        out.close();
        int responseCode = connection.getResponseCode();
        LOG.debug("Service returned response code " + responseCode);

    }
}
