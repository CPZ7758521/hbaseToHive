package com.pandora.www.hbase2hive.service;

import com.pandora.www.hbase2hive.config.Config;
import com.pandora.www.hbase2hive.utils.FileUtils;
import com.pandora.www.hbase2hive.utils.HbaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 处理步骤：
 * 1.清理集群中的临时文件，并创建一个临时目录，分两个表名，放到临时目录下。
 * 2.Hbase中的结构是，成交的数据一个列族，报价的数据一个列族。rowkey是每天，每天的数据在一个二维数组中。
 *   那么一个列族中就一列，一列中的一行就是一天的成交数据。
 *
 */
public class Hbase2HiveServiceImpl implements Hbase2HiveService {

    private static Logger LOG = LoggerFactory.getLogger(Hbase2HiveServiceImpl.class);
    public Map<String, String> hbaseData2Hive(String startDate, String endDate) throws IOException {
        LOG.info("-------------Start deal with the data from----------" + startDate + "--To--" + endDate + "--");
        HashMap<String, String> resultMap = new HashMap<>();

        String lastDataDate = endDate;
        long inRowCount = 0;
        FileUtils.deleteFile(Config.bjTableName);
        FileUtils.deleteFile(Config.cjTableName);

        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        LocalDate nowDate = LocalDate.parse(startDate, dateTimeFormatter);

        while (!nowDate.isAfter(LocalDate.parse(endDate))) {
            HbaseUtils
        }


        return resultMap;

    }
}
