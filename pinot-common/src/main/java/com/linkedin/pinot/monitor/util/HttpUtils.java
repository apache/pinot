package com.linkedin.pinot.monitor.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by johnliu on 15/7/23.
 */
public class HttpUtils {
    public static ObjectMapper mapper=new ObjectMapper();

    /**
     * {
     text: "文本",
     attachments: [{
     title: "标题",
     description: "描述",
     url: "链接",
     color: "warning|info|primary|error|muted|success"
     }]
     displayUser: {
     name: "机器人名称",
     avatarUrl: "头像地址"
     }
     }
     * @param text
     * @return
     */

    public static void postMonitorData(String text){
        SSLContext sslContext=null;
        HttpClient client=new DefaultHttpClient();
        try {
            sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null,
                    new TrustManager[] { new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws java.security.cert.CertificateException {

                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates, String s) throws java.security.cert.CertificateException {

                        }
                        @Override
                        public  X509Certificate[] getAcceptedIssuers(){
                            return null;
                        }
                    } }, new SecureRandom());
        }catch(Exception e){
            e.printStackTrace();
        }

        SSLSocketFactory ssf = new SSLSocketFactory(sslContext,SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        ClientConnectionManager ccm = client.getConnectionManager();
        SchemeRegistry sr = ccm.getSchemeRegistry();
        sr.register(new Scheme("https", 443, ssf));


        HttpPost httpPost = new HttpPost("https://hooks.pubu.im/services/1d2d2rwn8wb6sx");

        Map<String,Object> map=new HashMap<String,Object>();
        Map<String,String> sender=new HashMap<String,String>();
        sender.put("name","Monitor");
        map.put("displayUser",sender);
        List<String> list=new ArrayList<String>();
        map.put("attachments",list);

        try{
            map.put("text",text);
            InputStreamEntity httpentity = new InputStreamEntity(new ByteArrayInputStream(mapper.writeValueAsBytes(map)),mapper.writeValueAsBytes(map).length);
            httpPost.setEntity(httpentity);
            httpPost.addHeader("Content-Type","application/json");
            HttpResponse response=client.execute(httpPost);
            String result= EntityUtils.toString(response.getEntity());
            System.out.println(result);

        }catch(Exception e){
            e.printStackTrace();
        }
        finally{
            //
        }

    }



    public static void main(String args[]){
        postMonitorData("testing");
    }

}
