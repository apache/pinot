package org.apache.pinot.plugin.inputformat.avro.datahub.util;

import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a simple http util with connection pool to send get and post request
 */
public class HttpUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpUtil.class);

    // pool manager
    private static PoolingHttpClientConnectionManager poolConnManager = null;

    private static CloseableHttpClient httpClient;

    static {
        try {
            LOGGER.info("start initial http client......");
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
            // both support HTTP and HTPPS
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslsf).build();
            poolConnManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            // max connection numbers
            poolConnManager.setMaxTotal(640);
            poolConnManager.setDefaultMaxPerRoute(320);
            httpClient = getConnection();

            LOGGER.info(" initial http client successfully......");
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public static CloseableHttpClient getConnection() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(5000)
                .setConnectionRequestTimeout(5000).setSocketTimeout(5000).build();
        return HttpClients.custom()
                // set connection pool
                .setConnectionManager(poolConnManager)
                .setDefaultRequestConfig(config)
                // set retry times
                .setRetryHandler(new DefaultHttpRequestRetryHandler(2, false)).build();
    }

    public static UrlEncodedFormEntity getUrlEncodedFormEntity(JSONObject jsonObject) {
        List<NameValuePair> params = new ArrayList<>();
        if (jsonObject != null) {
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
            }
        }
        UrlEncodedFormEntity entity = null;
        try {
            entity = new UrlEncodedFormEntity(params);
        } catch (Exception e) {
            LOGGER.error("getUrlEncodedFormEntity error", e);
        }
        return entity;
    }

    public static StringEntity getStringEntity(JSONObject jsonObject) {
        String content = Optional.ofNullable(jsonObject.toString()).orElse("{}");
        return getStringEntity(content);
    }

    public static StringEntity getStringEntity(String str) {
        StringEntity entity = new StringEntity(str, "UTF-8");
        entity.setContentType("application/json");
        return entity;
    }

    public static String post(String uri, JSONObject object, Map<String, String> header) {
        Map<String, String> map = Optional.ofNullable(header).orElse(new HashMap<>());
        String contentType = map
                .getOrDefault("Content-Type", "application/json");
        AbstractHttpEntity entity;
        switch (contentType) {
            case "application/x-www-form-urlencoded":
                entity = getUrlEncodedFormEntity(object);
                break;
            case "application/json":
            default:
                entity = getStringEntity(object);
        }
        return post(uri, entity, map);
    }

    public static String post(String uri, AbstractHttpEntity entity, Map<String, String> header) {
        HttpPost httpPost = new HttpPost(uri);
        CloseableHttpResponse response = null;
        try {
            httpPost.setEntity(entity);
            if (header != null && !header.isEmpty()) {
                header.forEach(httpPost::addHeader);
            }
            response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity());
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                LOGGER.error("request {} error:{}, body:{}, {}", uri, code, EntityUtils.toString(entity), result);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("http post exception ", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                LOGGER.error("close exception ", e);
            }
        }
        return null;
    }

    public static String get(String uri, Map<String, String> header) {
        HttpGet httpGet = new HttpGet(uri);
        CloseableHttpResponse response = null;
        try {
            if (header != null && !header.isEmpty()) {
                header.forEach(httpGet::addHeader);
            }
            response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            String result = EntityUtils.toString(response.getEntity());
            if (code == HttpStatus.SC_OK) {
                return result;
            } else {
                LOGGER.error("request {} error:{}, {}", uri, code, result);
                return null;
            }
        } catch (IOException e) {
            LOGGER.error("http get exception ", e);
        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static void closeConnection() throws IOException {
        httpClient.close();
    }

}
