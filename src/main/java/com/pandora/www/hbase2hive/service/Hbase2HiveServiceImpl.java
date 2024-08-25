package com.pandora.www.hbase2hive.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public Map<String, String> hbaseData2Hive(String startDate, String endDate) {
        LOG.info("-------------Start deal with the data from----------" + startDate + "--To--" + endDate + "--");
        HashMap<String, String> resultMap = new HashMap<>();

        String lastDataDate = endDate;
        long inRowCount = 0;


        return resultMap;

    }
}
