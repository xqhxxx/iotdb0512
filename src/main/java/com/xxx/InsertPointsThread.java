package com.xxx;

import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/17
 * @apiNote
 *
 * 但此次插入测点数据
 * 数据为1个测点1条数据
 * 数据量由 batchSize 控制
 */
public class InsertPointsThread implements Runnable {
    private static SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);

    private final int batchSize;

    public InsertPointsThread( int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            String deviceId = "root.node.d1";

            List<Long> times = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<TSDataType>> typesList = new ArrayList<>();
            List<List<Object>> valuesList = new ArrayList<>();

            //一次写1测点 10w data
            long ct_l = System.currentTimeMillis();
            for (int i = 1; i <= batchSize; i++) {
                String point = "mp" + 1;
                Double AV = Math.random();

                List<String> measurements = new ArrayList<>();
                measurements.add(point);

                List<Object> values = new ArrayList<>(1);
                List<TSDataType> types = new ArrayList<>(1);
                values.add(AV);
                types.add(TSDataType.DOUBLE);

                times.add(ct_l+i);
                measurementsList.add(measurements);
                typesList.add(types);
                valuesList.add(values);
            }
            long start_time = System.currentTimeMillis();


            System.out.println(ct_l);
            System.out.println(start_time);
            poolSession.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList, false);
            System.out.println(Thread.currentThread().getName()+" insert  " + batchSize + " 测点 " + batchSize + " data   cost：" + (System.currentTimeMillis() - start_time));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
