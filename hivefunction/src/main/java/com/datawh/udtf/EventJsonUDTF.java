package com.datawh.udtf;

import org.apache.commons.lang.StringUtils;
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

public class EventJsonUDTF extends GenericUDTF{
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)  {
        List<String> fieldNames=new ArrayList<>();
        List<ObjectInspector> fieldTypes=new ArrayList<>();
//        依次定义，炸裂出的数据的字段名和字段类型
        fieldNames.add("event_name");
        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldTypes);
    }

    @Override
    public void process(Object[] args) {
        String jsons = args[0].toString();
        if(StringUtils.isBlank(jsons)){
        //再次校验传入数据
           return ;
        }else {
            try {
//                数组形式的json，用JSONArray接收处理
                JSONArray jsonArray = new JSONArray(jsons);

                for (int i = 0; i < jsonArray.length(); i++) {
                    String[] values=new String[2];
                    values[0]=jsonArray.getJSONObject(i).getString("en");
                    values[1]=jsonArray.getJSONObject(i).toString();
//                    forward写出，以行的形式，依次写出
                    forward(values);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (HiveException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
