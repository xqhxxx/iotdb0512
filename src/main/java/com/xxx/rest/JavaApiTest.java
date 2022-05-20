package com.xxx.rest;

import com.xxx.InsertPointsThread;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * @author xqh
 * @date 2022/5/20
 * @apiNote
 *
 * 测试查询
 */
public class JavaApiTest {
        //private static SessionPool poolSession = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
        public static void main(String[] args) throws Exception {

            //while (true)
            /// 单批次1w 数据 cost 10-20ms
            //insertPoints(1000, 10000);

            //多线程 读取数据 batchSize个测点（测点一条数据）
            ExecutorService pool = newFixedThreadPool(3);
            for (int i = 0; i < 1000; i++) {
                pool.submit(new QueryPointsThread(i));
                //pool.submit(new QueryPointsThread(1*1000));
            }
        }


}
