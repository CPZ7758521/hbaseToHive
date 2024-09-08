package com.pandora.www.hbase2hive.service;

import com.alibaba.fastjson.JSONArray;
import com.pandora.www.hbase2hive.config.Config;
import com.pandora.www.hbase2hive.utils.FileUtils;
import com.pandora.www.hbase2hive.utils.HbaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
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
            Map<String, List<JSONArray>> hbaseRowkeyRow = HbaseUtils.getHbaseRowkeyRow(dateTimeFormatter.format(nowDate));
            if (hbaseRowkeyRow.isEmpty()) {

                LOG.info("today did not have any data");
                nowDate.plusDays(1);
                continue;
            } else {
                lastDataDate = dateTimeFormatter.format(nowDate);

                inRowCount++;
                for (String family : hbaseRowkeyRow.keySet()) {
                    String familyName = "";

                    if ("cj".equals(family)) {
                        familyName = Config.cjTableName;
                    } else if ("bj".equals(family)) {
                        familyName = Config.bjTableName;
                    }

                    FileUtils.appendFile(familyName, hbaseRowkeyRow.get(family), dateTimeFormatter.format(nowDate));

                    LOG.info(familyName + " table date of " + dateTimeFormatter.format(nowDate) + "data is complete to CSV file");

                }

                nowDate.plusDays(1);
            }
        }

        LOG.info("start to push the data of " + Config.cjTableName + "to hdfs File System");
        FileUtils.flushFile(Config.cjTableName);

        LOG.info("start to push the data of " + Config.bjTableName + "to hdfs File System");
        FileUtils.flushFile(Config.bjTableName);

        LOG.info("data push complete");

        FileUtils.deleteFile(Config.cjTableName);
        FileUtils.deleteFile(Config.bjTableName);

        LOG.info("tmp file is deleted");

        resultMap.put("lastDataDate", lastDataDate);
        resultMap.put("inRowCount", String.valueOf(inRowCount));

        return resultMap;

    }
}
