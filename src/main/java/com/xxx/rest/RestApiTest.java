package com.xxx.rest;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/19
 * @apiNote
 */
public class RestApiTest {
    public static void main(String[] args) throws  Exception {


        /*//todo 1 ping测试
        //创建HttpClient对象
        CloseableHttpClient createDefault = HttpClients.createDefault();
        //創建get請求
        HttpGet get = new HttpGet("http://127.0.0.1:18080/ping");
        //執行請求
        CloseableHttpResponse execute = createDefault.execute(get);
        //判断响应状态码,为200输出内容
        if(execute.getStatusLine().getStatusCode()==200) {
            String string = EntityUtils.toString(execute.getEntity());
            System.out.println(string);
        }
        //关闭连接
        if(execute!=null) {
            execute.close();
        }
        //关闭连接
        if(createDefault!=null) {
            createDefault.close();
        }*/


    //    todo 2 post 侧测试数据

        //创建HttpClient,相当于浏览器
        CloseableHttpClient httpClient = HttpClients.createDefault();
        //创建post请求
        HttpPost post=new HttpPost("http://127.0.0.1:18080/rest/v1/query");

        post.setHeader("User-Agent","Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36");
        post.setHeader("Authorization","Basic cm9vdDpyb290");
        post.setHeader("Content-Type","application/json");

        //将参数存入map集合中
        List<NameValuePair> params=new ArrayList<NameValuePair>();
        params.add(new BasicNameValuePair("sql", "select mp1 from root.node.d1 limit 1"));
        //将map集合装换为form表单式的实体
        UrlEncodedFormEntity urlEncodedFormEntity = new UrlEncodedFormEntity(params);
        //将表单实体设置到post请求中
        post.setEntity(urlEncodedFormEntity);
        //执行请求
        CloseableHttpResponse execute = httpClient.execute(post);
        System.out.println(EntityUtils.toString(execute.getEntity()));
        execute.close();
        httpClient.close();

    }
}
