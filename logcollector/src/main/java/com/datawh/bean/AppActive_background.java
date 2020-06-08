package com.datawh.bean;

public class AppActive_background {
    private String active_source;//1=upgrade,2=download(下载),3=plugin_upgrade

    public void setActive_source(String active_source) {
        this.active_source = active_source;
    }

    public String getActive_source() {
        return active_source;
    }
}
