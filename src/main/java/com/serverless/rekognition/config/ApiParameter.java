package com.serverless.rekognition.config;

public class ApiParameter {

    public enum RequestType {
        REGISTER("register"),
        SEARCH("search"),
        UNKNOWN("");

        private String type;

        RequestType(String type) {
            this.type = type;
        }

        public String type() {
            return type;
        }
    }
    //getSignUrl
    public interface GetSignURL {
        String PATH_TYPE = "pathType";
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

    public interface PostUserData {
        String FIRSTNAME = "firstName";
        String SURENAME = "sureName";
        String NICKNAME = "nickName";
        String EMAIL = "email";
        String BIRTHDAY = "birthDay";
        String PHONE = "phone";
        String DESCRIPTION = "description";
        String IMAGEURL = "imageUrl";
        String BUCKET = "bucket";
        String KEY = "key";
    }
}
