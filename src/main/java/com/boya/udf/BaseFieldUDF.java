package com.boya.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * UDF用于解析公共字段
 */
public class BaseFieldUDF extends UDF {

    public String evaluate(String line, String jsonkeysString) {

        // 0 准备一个sb
        StringBuilder sb = new StringBuilder();

        // 1 切割jsonkeys（mid uid vc vn l sr os ar md...），获取所有key（公共字段名）
        // 公共字段数组
        String[] jsonkeys = jsonkeysString.split(",");

        // 2 处理line 服务器时间|json
        String[] logContents = line.split("\\|");

        // 3 合法性检验
        if (logContents.length != 2 || StringUtils.isBlank(logContents[1])) {
            return "";
        }

        try {
            // 4 开始处理json（对LogContents[1]创建json对象）
            JSONObject jsonObject = new JSONObject(logContents[1]);

            // 5 获取公共字段cm里面的对象（公共字段json对象）
            JSONObject cmJson = jsonObject.getJSONObject("cm");

            // 6 循环遍历
            // 对公共字段数组循环遍历
            for (int i = 0; i < jsonkeys.length; i++) {
                // 拿到每一个公共字段名
                String jsonkey = jsonkeys[i].trim();

                // 判断公共字段json对象中是否包含某个公共字段名
                if (cmJson.has(jsonkey)) {
                    // 向字符串缓冲区中拼接根据字段名获取到的值
                    sb.append(cmJson.getString(jsonkey)).append("\t");
                } else {
                    sb.append("\t");
                }
            }

            // 7 拼接事件字段和服务器时间
            // jsonObject.getString("et") 获取json对象中字段名为et（事件）的值
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(logContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    public static void main(String[] args) {

        String line = "1541217850324|{\"cm\":{\"mid\":\"m7856\",\"uid\":\"u8739\",\"ln\":\"-74.8\",\"sv\":\"V2.2.2\",\"os\":\"8.1.3\",\"g\":\"P7XC9126@gmail.com\",\"nw\":\"3G\",\"l\":\"es\",\"vc\":\"6\",\"hw\":\"640*960\",\"ar\":\"MX\",\"t\":\"1541204134250\",\"la\":\"-31.7\",\"md\":\"huawei-17\",\"vn\":\"1.1.2\",\"sr\":\"O\",\"ba\":\"Huawei\"},\"ap\":\"weather\",\"et\":[{\"ett\":\"1541146624055\",\"en\":\"display\",\"kv\":{\"goodsid\":\"n4195\",\"copyright\":\"ESPN\",\"content_provider\":\"CNN\",\"extend2\":\"5\",\"action\":\"2\",\"extend1\":\"2\",\"place\":\"3\",\"showtype\":\"2\",\"category\":\"72\",\"newstype\":\"5\"}},{\"ett\":\"1541213331817\",\"en\":\"loading\",\"kv\":{\"extend2\":\"\",\"loading_time\":\"15\",\"action\":\"3\",\"extend1\":\"\",\"type1\":\"\",\"type\":\"3\",\"loading_way\":\"1\"}},{\"ett\":\"1541126195645\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"0\",\"action\":\"2\",\"detail\":\"325\",\"source\":\"4\",\"behavior\":\"2\",\"content\":\"1\",\"newstype\":\"5\"}},{\"ett\":\"1541202678812\",\"en\":\"notification\",\"kv\":{\"ap_time\":\"1541184614380\",\"action\":\"3\",\"type\":\"4\",\"content\":\"\"}},{\"ett\":\"1541194686688\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"3\"}}]}";
        String x = new BaseFieldUDF().evaluate(line, "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);

    }

}
