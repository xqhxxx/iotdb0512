package com.xxx.rest;

import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/20
 * @apiNote
 */
public class QueryPointsThread implements Runnable {
    private static final SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    private final int batchSize;

    public QueryPointsThread( int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            String deviceId = "root.node.d1";

            //一次写1测点 10w data
            StringBuffer buffer=new StringBuffer();
            for (int i = 1; i <= batchSize; i++) {
                String point = "mp" + i;
                buffer.append(point).append(",");

            }

            StringBuffer buffer1 = buffer.delete(buffer.length() - 1, buffer.length());
            String sql="select "+buffer1+" from "+deviceId;

            long start_time = System.currentTimeMillis();
            System.out.println(start_time);

            SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeQueryStatement(sql);
            poolSession.closeResultSet(sessionDataSetWrapper);

            System.out.println(Thread.currentThread().getName()+" query ： " + batchSize + " 个测点， " + batchSize +
                    " 条data ，  cost：" + (System.currentTimeMillis() - start_time)+"ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
