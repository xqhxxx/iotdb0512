package com.xxx;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/12
 * @apiNote
 */
public class IoiTDBGet {
    private static SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    public static void main(String[] args) throws Exception {

        // 初始化Session
        //Session session = new Session("127.0.0.1", 6667, "root", "root");
        // 开启Session
        //session.open();

        Long cnt = 0L;
        //todo 1 原始数据查询。时间间隔包含开始时间，不包含结束时间
        long l = System.currentTimeMillis();
        String p1 = "root.node.d1.mp1";
        List<String> path = new ArrayList<>();
        path.add(p1);
        //path显示 默认1k 目前修改为10w
        SessionDataSetWrapper sessionDataSet = poolSession.executeRawDataQuery(path, l - 24*60*60 * 1000L, l);
        while (sessionDataSet.hasNext()) {
            RowRecord next = sessionDataSet.next();
            System.out.println(next);
            cnt++;
        }
        poolSession.closeResultSet(sessionDataSet);
        System.out.println(cnt);
        Thread.sleep(1000L);


        //todo 2 执行查询语句
        // 获取最新值
        SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeQueryStatement("select last  mp1  from root.node.d1");
        System.out.println(sessionDataSetWrapper.next());
        System.out.println(poolSession.executeQueryStatement("count timeseries root.node.d1").next());

    }
}
