package com.datawh.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    /*
    1549814494011 | {"cm": {"ln": "-84.4","sv": "V2.3.5",},
	"ap": "app",
	"et": [{"ett": "1549758825503",	"en": "newsdetail",
	            "kv": {"entry": "2","goodsid": "1",	}
	        }, {]  }
     */
    public String evaluate(String line, String key) {
        String[] log = line.split("\\|");

        //再次校验数据
        if (log.length != 2 || StringUtils.isBlank(log[1])) {
            return "";
        }

        //分离数据，得到servertime和et的json值
        JSONObject json = null;
        String result="";
        try {
            json = new JSONObject(log[1].trim());
            try {
                if ("et".equals(key)) {
                    if (json.has(key)) {
                        result= json.getString("et");
                    }
                } else if ("st".equals(key)) {
                    if (json.has(key)) {
                        result= log[0].trim();
                    }
                }else {
                    JSONObject cm = json.getJSONObject("cm");
                    if(cm.has(key)){
                        result=cm.getString(key);
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return result;
    }

}
