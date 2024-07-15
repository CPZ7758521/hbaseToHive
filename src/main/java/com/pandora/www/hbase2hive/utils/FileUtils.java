package com.pandora.www.hbase2hive.utils;

import com.alibaba.fastjson.JSONArray;
import com.pandora.www.hbase2hive.config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.config.Conf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.List;

public class FileUtils {

    private static Logger LOG = LoggerFactory.getLogger(FileUtils.class);

    private static Configuration conf;
    private static String keytabPath;
    private static String krb5Path;
    private static final String HDFS_BASE_PATH = "/user/test/hive_db/pandora.db/";
    private static final String LOCAL_BASE_PATH = System.getProperty("java.io.tmpdir") + "/" + System.getProperty("user.name") + "/hbase2hive";

    static {

        try {
            URL keytabUrl = FileUtil.class.getClassLoader().getResource(Config.env + "/pandora.keytab");
            URL krb5Url = FileUtil.class.getClassLoader().getResource(Config.env + "krb5.conf");

            keytabPath = keytabUrl.getPath();
            krb5Path = krb5Url.getPath();

            String protocol = krb5Url.getProtocol();

            //集群上运行时，copy keytab，krb5文件，方便kerberos认证
            if ("jar".equals(protocol)) {
                krb5Path = LOCAL_BASE_PATH + "/krb5.conf";
                keytabPath = LOCAL_BASE_PATH + "/pandora.keytab";

                File dir = new File(LOCAL_BASE_PATH);
                if (!dir.exists()) {
                    dir.mkdir();
                }

                FileOutputStream krb5Fos = new FileOutputStream(krb5Path);
                FileOutputStream keytabFos = new FileOutputStream(keytabPath);

                IOUtils.copy(keytabUrl, keytabFos);
                IOUtils.copy(krb5Url, krb5Fos);
            }

            LOG.info("krb5conf Path: " + krb5Path);
            LOG.info("keytab Path: " + keytabPath);

            System.setProperty("java.security.krb5.conf", krb5Path);

            conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            conf.addResource(Config.env + "/core-site.xml");
            conf.addResource(Config.env + "/hdfs-site.xml");
            conf.addResource(Config.env + "hive-site.xml");

            conf.set("hadoop.sercurity.authentication", "kerberos");
            //在整个项目中，需要链接两个hadoop集群，因此，在使用keytab的时候才去调用。
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(Config.kerberosUsername, keytabPath);
        } catch (IOException e) {
            LOG.error("kerberos authentication failure: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void deleteFile(String tableName) throws IOException {
        //delete local file in temp dir
        File dir = new File(LOCAL_BASE_PATH + File.separator + tableName);
        org.apache.commons.io.FileUtils.deleteDirectory(dir);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }

    public static void appendFile(String tableName, List<JSONArray> tList, String rowkey) throws IOException {
        FileWriter fw = new FileWriter(LOCAL_BASE_PATH + "/" + tableName + "db-" + rowkey + "-" + tList.size() + ".csv", true);
        BufferedWriter bw = new BufferedWriter(fw);

        for (JSONArray t : tList) {
            bw.write(StringUtils.toCsvString(t, rowkey));
            bw.newLine();
        }

        fw.flush();
        bw.flush();
        bw.close();
        fw.close();
    }

    public static void flushFile(String tableName) throws IOException {
        File dir = new File(LOCAL_BASE_PATH + "/" + tableName);

        File[] listFiles = dir.listFiles();
        if (listFiles.length > 0) {
            //如果有多天的数据，将所有文件聚合成一个db文件
            if (listFiles.length > 1 || !"db.csv".equals(listFiles[0].getName())) {
                for (File listFile : listFiles) {
                    FileInputStream fis = new FileInputStream(listFile);
                    FileOutputStream fos = new FileOutputStream(LOCAL_BASE_PATH + "/" + tableName + "db.csv", true);

                    IOUtils.copy(fis, fos);

                    fos.close();
                    fis.close();
                    listFile.delete();
                }
            }

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(Config.kerberosUsername, keytabPath);

            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path(LOCAL_BASE_PATH + "/" + tableName + "/db.csv"), new Path(HDFS_BASE_PATH + tableName));

            fs.close();
        }
    }

}
