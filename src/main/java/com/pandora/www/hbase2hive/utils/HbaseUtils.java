package com.pandora.www.hbase2hive.utils;

import com.alibaba.fastjson.JSONArray;
import com.pandora.www.hbase2hive.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseUtils {

    private static Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

    public static Connection getHbaseConnection() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "cpz001");

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Config.zookeeperIp);
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConnect = ConnectionFactory.createConnection(config);

        return hbaseConnect;
    }

    public static Map<String, List<JSONArray>> getHbaseRowkeyRow(String rowkey) throws IOException {

        Connection hbaseConnect = getHbaseConnection();

        Table table = hbaseConnect.getTable(TableName.valueOf(Config.hbaseTable));

        Get get = new Get(rowkey.getBytes());

        //拿到一行
        Result result = table.get(get);

        HashMap<String, List<JSONArray>> familyMap = new HashMap<>();

        if (result.isEmpty()) {
            return null;
        }

        //列族 下还有列
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();

        //遍历列族
        for (byte[] family : map.keySet()) {
            ArrayList<JSONArray> resultList = new ArrayList<>();

//            拿到列族对应的 列：cell map
            for (byte[] column : map.get(family).keySet()) {
                resultList.addAll(StringUtils.stringToArray(Bytes.toString(map.get(family).get(column)), rowkey));
            }
//            多个列族对应的数据合并成一行数据
            familyMap.put(Bytes.toString(family), resultList);
        }
        hbaseConnect.close();
        return familyMap;
    }

    public static long rowCountByScanFilter() {
        LOG.info("start to caculate the count of hbase table one day");

        long rowCount = 0L;

        try {
            Connection connection = getHbaseConnection();
            Table table = connection.getTable(TableName.valueOf(Config.hbaseTable));

            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner results = table.getScanner(scan);
            for (Result result : results) {
                rowCount += result.size();
            }

            results.close();
            table.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return rowCount;
    }
}
