package com.pandora.www.hbase2hive.service;

import java.util.Map;

public interface Hbase2HiveService {
    Map<String, String> hbaseData2Hive(String startDate, String endDate);
}
