package com.datawh.interceptor;

import com.datawh.utils.ETLUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    private List<Event> list = new ArrayList<>();

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String log = event.getBody().toString();
        boolean flag =false;
        if (log.contains("start")) {
            flag = ETLUtils.etlStart(log);
        } else if (log.contains("event")) {
            flag = ETLUtils.etlEvent(log);
        }
        if (flag) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        list.clear();
        for (Event event : events) {
            Event etlEvent = intercept(event);
            if (etlEvent != null) {
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
