package com.xxx;

import com.magus.jdbc.net.OPSubscribe;
import com.magus.jdbc.net.SubscribeResultSet;
import com.magus.net.OPConnect;
import com.magus.net.OPStaticInfo;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
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

/**
 * @author xqh
 * @date 2022/5/12
 * @apiNote
 */
public class IoTDBMagusTest implements SubscribeResultSet {
    public static void main(String[] args) throws Exception {


        Class.forName("com.magus.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:openplant://10.191.177.6:8200/RTDB", "sis", "openplant");
        if (conn.getAutoCommit()) {
            conn.setAutoCommit(false);
        }
        String table_name = "Realtime";
        ArrayList<Integer> ids = new ArrayList<>();
//                ids.add(ProUtils.getIntValue(Configs.OP_NODE_ID));//4 三峡
        ids.add(2);//535559
        OPSubscribe subscribe = new OPSubscribe(conn, table_name, ids, new IoTDBMagusTest());
        Thread.sleep(1000 * 2);
        //conn.rollback();
        conn.close();

    }

    //private static Session session;
    private static SessionPool poolSession;

    static {

        poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 10);

        // 初始化Session
        //session = new Session("127.0.0.1", 6667, "root", "root");
        // 开启Session
//        try {
//            //session.open();
//            // 设置存储组--这里注意不能设置已经存在的存储组，
//            poolSession.setStorageGroup("root.node1");
//            //创建时间序列
//            // 初始化测点
//            OPConnect conn = null;
//            conn = new OPConnect("10.191.177.6", 8200, 60000, "sis", "openplant");
//            OPStaticInfo[] ops = conn.getAllPointStaticInfosByNodeName("W3.GZB");
////                   "W4.SXDXTEST");
//            for (int i = 0; i < ops.length; i++) {
//                OPStaticInfo op = ops[i];
//                String pn = op.getPN();
//                poolSession.createTimeseries( "root.node1.gzb."+pn, TSDataType.DOUBLE, TSEncoding.RLE, CompressionType.SNAPPY);
//            }
//
//            //288108  144054个测点
//
//            //failed to insert points [W3.GZB.01001ARB11GD101------ADI1BLIN.tm, W3.GZB.01001ARB11GD101------ADI1BLIN.av, W3.GZB.01001ARB11GD101-CJB10ADI1ACT.tm, W3.GZB.01001ARB11GD101-CJB10ADI1ACT.av, W3.GZB.01001ARB11GD101-CJB10ADI1BLIN.tm, W3.GZB.01001ARB11GD101-CJB10ADI1BLIN.av, W3.GZB.01001ARB11GD101-CJB10ADI1GRC.tm, W3.GZB.01001ARB11GD101-CJB10ADI1GRC.av, W3.GZB.01001ARB11GD101-CJB10ADI1LCK.tm, W3.GZB.01001ARB11GD101-CJB10ADI1LCK.av, W3.GZB.01001ARB11GD101-CJB10ADI1OVC.tm, W3.GZB.01001ARB11GD101-CJB10ADI1OVC.av, W3.GZB.01001ARB11GD101-CJB10ADI1OVL.tm, W3.GZB.01001ARB11GD101-CJB10ADI1OVL.av, W3.GZB.01001ARB11GD101-CJB10ADI1OVV.tm, W3.GZB.01001ARB11GD101-CJB10ADI1OVV.av, W3.GZB.01001ARB11GD101-CJB10ADI1PROT.tm, W3.GZB.01001ARB11GD101-CJB10ADI1PROT.av, W3.GZB.01001ARB11GD101-CJB10ADI2ACT.tm, W3.GZB.01001ARB11GD101-CJB10ADI2ACT.av] caused by W3.GZB.01001ARB11GD101-CJB10ADI2ACT.tm is an illegal measurementId;
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    /**
     * 一： 测点
     * 测点时间 直接测点值
     * */
    @Override
    public void onResponse(ResultSet result) throws SQLException {

        try {
            String deviceId = "root.node1.gzb";

            List<Long> times = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<TSDataType>> typesList = new ArrayList<>();
            List<List<Object>> valuesList = new ArrayList<>();

            //long ct_l = System.currentTimeMillis();

            //处理订阅到的数据结果，
            int cnt = 0;
            while (result.next()) {
                String GN = result.getString(2);
                Long TM = result.getDate(3).getTime();
                Double AV = Double.valueOf(result.getString(5));

                //deviceIdList.add(deviceId + "." + GN);

                List<String> measurements = new ArrayList<>();
                measurements.add(GN.split("\\.")[2]);

                List<Object> values = new ArrayList<>(1);
                List<TSDataType> types = new ArrayList<>(1);
                values.add(AV);
                types.add(TSDataType.DOUBLE);

                times.add(TM);
                measurementsList.add(measurements);
                typesList.add(types);
                valuesList.add(values);

                cnt++;
            }

            // 一个设备 多record
            //String deviceId, List<Long > times, List < List < String >> measurementsList, List < List < TSDataType >> typesList, List < List < Object >> valuesList
            //session.insertRecordsOfOneDevice(deviceId, times,measurementsList,typesList,valuesList);
            poolSession.insertOneDeviceRecords(deviceId, times, measurementsList, typesList, valuesList);

            System.out.println("size:" + cnt);


            //cli  查询

            //session.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}


//数据类型	支持的编码
//BOOLEAN	PLAIN, RLE
//INT32	PLAIN, RLE, TS_2DIFF, GORILLA
//INT64	PLAIN, RLE, TS_2DIFF, GORILLA
//FLOAT	PLAIN, RLE, TS_2DIFF, GORILLA
//DOUBLE	PLAIN, RLE, TS_2DIFF, GORILLA
//TEXT	PLAIN
