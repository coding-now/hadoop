package com.bigdata.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by WangBin on 2017/8/21.
 */
public class HttpClientUtil {
    /*public static final MediaType JSON = MediaType.parse("application/json;charset=utf-8");
    private static OkHttpClient httpClient = new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)       //设置连接超时
            .readTimeout(10, TimeUnit.SECONDS)          //设置读超时
            .writeTimeout(10,TimeUnit.SECONDS)          //设置写超时
            .retryOnConnectionFailure(true)             //是否自动重连
            .build();                                   //构建OkHttpClient对象
    */

    public static String doPost(String url, Map<String,String> map, String charset){
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try{
            if(url.startsWith("https://")) {
                httpClient = HttpClients.custom().setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
            }else if(url.startsWith("http://")){
                httpClient = new DefaultHttpClient();
            }
            httpPost = new HttpPost(url);
            if(map!=null) {
                //设置参数
                List<NameValuePair> list = new ArrayList<NameValuePair>();
                Iterator iterator = map.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> elem = (Map.Entry<String, String>) iterator.next();
                    list.add(new BasicNameValuePair(elem.getKey(), elem.getValue()));
                }
                if (list.size() > 0) {
                    UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list, charset);
                    httpPost.setEntity(entity);
                }
            }
            HttpResponse response = httpClient.execute(httpPost);
            if(response != null){
                HttpEntity resEntity = response.getEntity();
                if(resEntity != null){
                    result = EntityUtils.toString(resEntity,charset);
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }
    /*public static String httpGet(String url) throws IOException {
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
    }*/
}
