package com.xxx;

import com.magus.jdbc.net.OPSubscribe;
import com.magus.jdbc.net.SubscribeResultSet;
import com.magus.net.OPStaticInfo;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * @author xqh
 * @date 2022/5/16
 * @apiNote 测试 10w 20w 50w
 */
public class IoTDBTest_w {
    private static SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    public static void main(String[] args) throws Exception {
        //insertData(1);//*直接单批次 1*10w  后续写 本机扛不住
        //insertData(2);
        //insertData(5);
        //insertOnePoint();

        //while (true)
        /// 单批次1w 数据 cost 10-20ms
        //insertPoints(1000, 10000);

        //多线程 写入 batchSize个测点（测点一条数据）
        ExecutorService pool = newFixedThreadPool(3);
        for (int i = 0; i < 1; i++) {
            pool.submit(new InsertPointsThread(500 * 1000));
        }
// 本机环境  模拟创建10w个测点。
//多线程测试写入数据，
//单批次数据量    花费时间
//    2w        37-70ms
//    1w        20-40ms
//    5k        8-20ms
//    1k        0-7ms
//单批次数据量2w再往上，比如：10w直接后续扛不住卡死。
    }

    /*static {
        try {
            poolSession.setStorageGroup("root.node");
            //创建时间序列
            // 初始化测点 10w测点
            for (int i = 1; i <= 100*1000; i++) {
                String measurePoint = "mp"+i;
                poolSession.createTimeseries( "root.node.d1."+measurePoint, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    /**
     * 一次写入
     * 10w 测点 size*10w data
     */
    public static void insertData(int size) throws Exception {

        String deviceId = "root.node.d1";

        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();


        while (true) {

            //一次写10w测点
            long ct_l = System.currentTimeMillis();
            for (int i = 1; i <= 10 * 10000; i++) {

                for (int j = 0; j < size; j++) {
                    String point = "mp" + i;
                    Double AV = Math.random();

                    List<String> measurements = new ArrayList<>();
                    measurements.add(point);

                    List<Object> values = new ArrayList<>(1);
                    List<TSDataType> types = new ArrayList<>(1);
                    values.add(AV);
                    types.add(TSDataType.DOUBLE);

                    times.add(ct_l + j);
                    measurementsList.add(measurements);
                    typesList.add(types);
                    valuesList.add(values);
                }

            }
            long start_time = System.currentTimeMillis();
            System.out.println(ct_l);
            System.out.println(start_time);
            poolSession.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList);
            System.out.println("insert 10w 测点  cost：" + (System.currentTimeMillis() - start_time));

            Thread.sleep(2000L);
        }

    }

    /**
     * @throws Exception insert 1测点 10w data
     */
    public static void insertOnePoint() throws Exception {

        String deviceId = "root.node.d1";

        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();

        while (true) {
            //一次写1测点 10w data
            long ct_l = System.currentTimeMillis();
            Integer size = 100 * 1000;
            for (int i = 1; i <= size; i++) {

                String point = "mp1";
                Double AV = Math.random();

                List<String> measurements = new ArrayList<>();
                measurements.add(point);

                List<Object> values = new ArrayList<>(1);
                List<TSDataType> types = new ArrayList<>(1);
                values.add(AV);
                types.add(TSDataType.DOUBLE);

                times.add(ct_l + i);
                measurementsList.add(measurements);
                typesList.add(types);
                valuesList.add(values);
            }
            long start_time = System.currentTimeMillis();
            System.out.println(ct_l);
            System.out.println(start_time);
            poolSession.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList, false);
            System.out.println("insert 1测点 " + size + " data   cost：" + (System.currentTimeMillis() - start_time));

            Thread.sleep(2000L);
        }

        //cli  查询
        //session.close();

    }


    /**
     * 初始化了10w 测点
     *
     * @param dataSize  写入数据量
     * @param batchSize 每批次大小
     * @throws Exception 批次当做测点
     */
    public static void insertPoints(int dataSize, int batchSize) throws Exception {

        String deviceId = "root.node.d1";

        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();

        //一次写1测点 10w data
        long ct_l = System.currentTimeMillis();
        for (int i = 1; i <= batchSize; i++) {
            String point = "mp" + i;
            Double AV = Math.random();

            List<String> measurements = new ArrayList<>();
            measurements.add(point);

            List<Object> values = new ArrayList<>(1);
            List<TSDataType> types = new ArrayList<>(1);
            values.add(AV);
            types.add(TSDataType.DOUBLE);

            times.add(ct_l);
            measurementsList.add(measurements);
            typesList.add(types);
            valuesList.add(values);
        }
        long start_time = System.currentTimeMillis();
        System.out.println(ct_l);
        System.out.println(start_time);
        poolSession.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList, false);
        System.out.println("insert  " + batchSize + " 测点 " + batchSize + " data   cost：" + (System.currentTimeMillis() - start_time));

        Thread.sleep(1000);
    }


}


//数据类型	支持的编码
//BOOLEAN	PLAIN, RLE
//INT32	PLAIN, RLE, TS_2DIFF, GORILLA
//INT64	PLAIN, RLE, TS_2DIFF, GORILLA
//FLOAT	PLAIN, RLE, TS_2DIFF, GORILLA
//DOUBLE	PLAIN, RLE, TS_2DIFF, GORILLA
//TEXT	PLAIN
