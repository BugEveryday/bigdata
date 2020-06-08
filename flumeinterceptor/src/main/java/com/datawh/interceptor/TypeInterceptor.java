package com.datawh.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {
    private List<Event> list = new ArrayList<>();
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String log = event.getBody().toString();
        Map<String, String> headers = event.getHeaders();
        if(log.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        list.clear();
        for (Event event : events) {
            Event etlEvent=intercept(event);
            if(etlEvent!=null){
                list.add(etlEvent);
            }
        }
        return list;
    }

    @Override
    public void close() {

    }
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
