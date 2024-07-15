package com.pandora.www.hbase2hive.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class StringUtils {


    //将二维数组中的一行数据，传入，然后返回 竖线 分隔的String
    public static String toCsvString(JSONArray jsonArray, String rowkey) {
        String str = rowkey + "|";

        boolean b = false;

        for (Object o : jsonArray) {
            str += ((o == null) ? "" : o) + "|";
        }

        str = str.replace("\\n", "");
        return str;
    }


    public static List<JSONArray> stringToArray(String str, String rowkey) {
        ArrayList<JSONArray> columnDatalist = new ArrayList<>();
        JSONArray mainArray = JSON.parseArray(str);
        for (int i = 0; i < mainArray.size(); i++) {
            JSONArray jsonArray = mainArray.getJSONArray(i);
            jsonArray.add(i + 1);
            columnDatalist.add(jsonArray);
        }
        return columnDatalist;
    }
}
