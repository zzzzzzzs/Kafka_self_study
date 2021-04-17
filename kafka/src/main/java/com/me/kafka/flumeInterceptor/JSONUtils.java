package com.me.kafka.flumeInterceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class JSONUtils {
    public static boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}
