package com.pandora.www.hbase2hive.utils;

import com.alibaba.fastjson.JSONArray;
import com.pandora.www.hbase2hive.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

public class HbaseUtils {
    public static Map<String, List<JSONArray>> getHbaseClient(String rowkey) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "cpz001");

        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Config.zookeeperIp);
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConnect = ConnectionFactory.createConnection(config);
        Table table = hbaseConnect.getTable(TableName.valueOf(Config.hbaseTable));

        Get get = new Get(rowkey.getBytes());

        Result result = table.get(get);

        HashMap<String, List<JSONArray>> familyMap = new HashMap<>();

        if (result.isEmpty()) {
            return null;
        }

        NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = result.getNoVersionMap();

        for (byte[] family : map.keySet()) {
            ArrayList<JSONArray> resultList = new ArrayList<>();
            map.get(family).keySet()

        }


    }
}
