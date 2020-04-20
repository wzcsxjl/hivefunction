package com.boya.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * 展开业务字段
 */
public class EventJsonUDTF extends GenericUDTF {

    // 指定输出的参数名称和参数类型
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("event_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("event_json");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 输入一条记录，输出若干条结果
    // 对et json数组进行处理 一个数组中含有多个事件
    // [{"ett":"1541146624055","en":"display","kv":{"copyright":"ESPN","content_provider":"CNN","extend2":"5","goodsid":"n4195","action":"2","extend1":"2","place":"3","showtype":"2","category":"72","newstype":"5"}},{"ett":"1541213331817","en":"loading","kv":{"extend2":"","loading_time":"15","action":"3","extend1":"","type1":"","type":"3","loading_way":"1"}},{"ett":"1541126195645","en":"ad","kv":{"entry":"3","show_style":"0","action":"2","detail":"325","source":"4","behavior":"2","content":"1","newstype":"5"}},{"ett":"1541202678812","en":"notification","kv":{"ap_time":"1541184614380","action":"3","type":"4","content":""}},{"ett":"1541194686688","en":"active_background","kv":{"active_source":"3"}}]
    @Override
    public void process(Object[] objects) throws HiveException {

        // 获取输入数据（传入的et）
        String input = objects[0].toString();

        // 如果传进来的数据为空，直接返回过滤掉该数据
        if (StringUtils.isBlank(input)) {
            return;
        } else {
            try {
                // 获取一共有几个事件（ad/favorites）
                // 将input json字符串转换为json数组
                JSONArray ja = new JSONArray(input);

                if (ja == null) {
                    return;
                }

                // 循环遍历每一个事件
                for (int i = 0; i < ja.length(); i++) {
                    // 创建一个长度为2的字符串数组
                    String[] result = new String[2];
                    try {
                        // 取出每个事件的名称（ad/favorites）
                        // getJSONObject(i).getString("en") 获取第i个事件的事件名
                        result[0] = ja.getJSONObject(i).getString("en");

                        // 取出每一个整体事件
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    // 将结果返回
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    // 当没有记录处理时该方法会被调用，用来清理代码或者产生额外的输出
    @Override
    public void close() throws HiveException {

    }
}