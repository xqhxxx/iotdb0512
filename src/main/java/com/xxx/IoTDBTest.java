package com.xxx;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xqh
 * @date 2022/5/12
 * @apiNote
 *
 * 参看官网：https://iotdb.apache.org/zh/UserGuide/V0.12.x/QuickStart/QuickStart.html
 *
 */
public class IoTDBTest {
    public static void main(String[] args) throws Exception {


        // 初始化Session
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        // 开启Session
        session.open();
        // 设置存储组--这里注意不能设置已经存在的存储组，不然会爆出异常   300: xxxx.xx has already been set to storage group
        //session.setStorageGroup("root.node1");
        //创建时间序列
        session.createTimeseries("root.node1.wf01.kks.gn", TSDataType.TEXT, TSEncoding.PLAIN, CompressionType.SNAPPY);
        session.createTimeseries("root.node1.wf01.wt02.av", TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
        session.createTimeseries("root.node1.wf01.wt02.dt", TSDataType.INT32, TSEncoding.RLE, CompressionType.SNAPPY);

        // 在这里可以选择已经存在的设备，可以通过show devices来查看自己的设备
        String deviceId = "root.node1.wf01.wt02";
        List<String> measurements = new ArrayList<>(16);
        measurements.add("gn");
        measurements.add("av");
        measurements.add("dt");

        //for (int i = 1000; i < 1010; i++) {
            List<Object> values = new ArrayList<>(3);
            List<TSDataType> types = new ArrayList<>(3);
            values.add("kks-03");
            types.add(TSDataType.TEXT);
            values.add(0.5);
            types.add(TSDataType.DOUBLE);
            values.add(20220512);
            types.add(TSDataType.INT32);

            // 插入多个记录，在这里提供了数据类型，服务器不需要做类型推断，可以提高性能
            //String deviceId, long time, List<String > measurements, List < TSDataType > types, List < Object > values)
            //session.insertRecord(deviceId, i, measurements, types, values);
        session.insertRecord(deviceId, 1000, measurements, types, values);
        //}
        //cli  查询

        session.close();


    }
}


//数据类型	支持的编码
//BOOLEAN	PLAIN, RLE
//INT32	PLAIN, RLE, TS_2DIFF, GORILLA
//INT64	PLAIN, RLE, TS_2DIFF, GORILLA
//FLOAT	PLAIN, RLE, TS_2DIFF, GORILLA
//DOUBLE	PLAIN, RLE, TS_2DIFF, GORILLA
//TEXT	PLAIN
