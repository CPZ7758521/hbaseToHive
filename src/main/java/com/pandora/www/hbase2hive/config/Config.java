package com.pandora.www.hbase2hive.config;

import com.alibaba.dcm.DnsCacheManipulator;

import java.io.IOException;
import java.util.Properties;

public class Config {
    public static String env;
    public static String kerberosUsername;
    public static String hiveJdbcUrl;
    public static String bjTableName;
    public static String cjTableName;
    public static String startDate;
    public static String endDate;
    public static String hbaseTable;
    public static String zookeeperIp;

    public static boolean getTableCount;

    static {
        Properties properties = new Properties();
        env = System.getProperty("env");
        startDate = System.getProperty("startDate");
        endDate = System.getProperty("endDate");
        hbaseTable = System.getProperty("hbaseTable");

        getTableCount = Boolean.valueOf(System.getProperty("getTableCount"));

        try {
            properties.load(Config.class.getClassLoader().getResourceAsStream(env + "/config.properties"));
            //处理虚拟dns
            DnsCacheManipulator.loadDnsCacheConfig(env + "/dns-cache.properties");


            kerberosUsername = properties.getProperty("kerberos.username");
            hiveJdbcUrl = properties.getProperty("hive.jdbc.url");
            bjTableName = properties.getProperty("bjTable");
            cjTableName = properties.getProperty("cjTable");
            zookeeperIp = properties.getProperty("zookeeperIp");

        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
