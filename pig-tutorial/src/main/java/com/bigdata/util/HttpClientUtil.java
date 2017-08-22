package com.bigdata.util;

import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by WangBin on 2017/8/21.
 */
public class HttpClientUtil {
        public static final MediaType JSON = MediaType.parse("application/json;charset=utf-8");
        private static OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(60, TimeUnit.SECONDS)       //设置连接超时
                .readTimeout(10, TimeUnit.SECONDS)          //设置读超时
                .writeTimeout(10,TimeUnit.SECONDS)          //设置写超时
                .retryOnConnectionFailure(true)             //是否自动重连
                .build();                                   //构建OkHttpClient对象


        public static String httpGet(String url) throws IOException {
            //OkHttpClient httpClient = new OkHttpClient();
            Request request = new Request.Builder()
                    .url(url)
                    .build();
            Response response = httpClient.newCall(request).execute();
            return response.body().string(); // 返回的是string 类型，json的mapper可以直接处理
        }

        public static String httpPost(String url, String json) throws IOException {
            //OkHttpClient httpClient = new OkHttpClient();
            RequestBody requestBody = RequestBody.create(JSON, json);
            Request request = new Request.Builder()
                    .url(url)
                    .post(requestBody)
                    .build();
            Response response = httpClient.newCall(request).execute();
            return response.body().string();
        }
}
