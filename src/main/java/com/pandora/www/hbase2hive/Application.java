package com.pandora.www.hbase2hive;

import com.pandora.www.hbase2hive.config.Config;
import com.pandora.www.hbase2hive.service.Hbase2HiveServiceImpl;
import com.pandora.www.hbase2hive.utils.HbaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Application {
    private static Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws IOException {
        LOG.info("start deal, envoriment is " + Config.env);
        long rowCount = 0L;

        if (Config.getTableCount) {
            rowCount = HbaseUtils.rowCountByScanFilter();
        }

        Hbase2HiveServiceImpl hbase2HiveService = new Hbase2HiveServiceImpl();
        Map<String, String> resultMap = hbase2HiveService.hbaseData2Hive(Config.startDate, Config.endDate);

        if (Config.getTableCount) {
            LOG.info("deal with complete, the count of table is " + rowCount);
        }
        LOG.info("deal with complete, the count of this time deal is " + resultMap.get("inRowCount"));
    }
}
