package com.xxx.rest;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.ArrayList;

/**
 * @author xqh
 * @date 2022/5/20
 * @apiNote
 */
public class QueryPointsThread implements Runnable {
    private static final SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    private final int batchSize;
    private final int codeSize;

    public QueryPointsThread(int batchSize, int codeSize) {
        this.batchSize = batchSize;
        this.codeSize = codeSize;
    }

    @Override
    public void run() {
        try {
            String deviceId = "root.node.d1";

            //一次写1测点 10w data
            StringBuffer buffer=new StringBuffer();

            ArrayList<String> paths = new ArrayList<>();

            for (int i = 1; i <= codeSize; i++) {
                String point = "mp" + i;
                buffer.append(point).append(",");

                paths.add(deviceId+"."+point);
            }

            StringBuffer buffer1 = buffer.delete(buffer.length() - 1, buffer.length());
            String sql="select "+buffer1+" from "+deviceId
                    +" limit "+batchSize;

            long start_time = System.currentTimeMillis();
            System.out.println(start_time);

            // 传：sql 查询 测点和数据量
//            SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeQueryStatement(sql);
//          传：测点 时间段区间
            SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeRawDataQuery(paths,
                    System.currentTimeMillis()-30*60*1000,System.currentTimeMillis());

            int dataSize=0;
//            sessionDataSetWrapper.getColumnNames().forEach(x-> System.out.println(x));
            while (sessionDataSetWrapper.hasNext()){
//                System.out.println(sessionDataSetWrapper.next());
                dataSize++;
            }
            poolSession.closeResultSet(sessionDataSetWrapper);

            System.out.println(Thread.currentThread().getName()+" query ： " + codeSize + " 个测点， " + dataSize +
                    " 条data ，  cost：" + (System.currentTimeMillis() - start_time)+"ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IoTDBConnectionException, StatementExecutionException {
        StringBuffer buffer=new StringBuffer();
        int dataSize=0;
        String deviceId = "root.node.d1";
        ArrayList<String> paths = new ArrayList<>();
        long start_time = System.currentTimeMillis();
        for (int i = 1; i <= 100000; i++) {
            String p="mp"+i;

            buffer.append(p).append(",");

            paths.add(deviceId+"."+p);

            //10w  总量  循环查询 总花费
            //单次1k列（测点） cost 3609ms
            //1w        3531ms
            //5w        3857
            //10w       3844

            if (i%1000*10==0) {
//                SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeRawDataQuery(paths,
//                        System.currentTimeMillis() - 25 * 60 * 1000, System.currentTimeMillis());

                StringBuffer buffer1 = buffer.delete(buffer.length() - 1, buffer.length());
                String sql="select "+buffer1+" from "+deviceId
                        +" limit 1";
                SessionDataSetWrapper sessionDataSetWrapper = poolSession.executeQueryStatement(sql);
                while (sessionDataSetWrapper.hasNext()) {
                    dataSize++;
                }
                System.out.println(dataSize);
                poolSession.closeResultSet(sessionDataSetWrapper);
                paths.clear();
                buffer.delete(0,buffer.length());
            }
        }

//        单次1000列（测点） 10w  循环100次  cost 3609ms

        System.out.println(Thread.currentThread().getName() + dataSize +
                " 条data ，  cost：" + (System.currentTimeMillis() - start_time)+"ms");
    }
}
