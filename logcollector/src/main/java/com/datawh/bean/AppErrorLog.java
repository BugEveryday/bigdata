package com.datawh.bean;

public class AppErrorLog {

    private String errorBrief;    //错误摘要
    private String errorDetail;   //错误详情
    public void setErrorBrief(String errorBrief) {
        this.errorBrief = errorBrief;
    }

    public void setErrorDetail(String errorDetail) {
        this.errorDetail = errorDetail;
    }

    public String getErrorBrief() {
        return errorBrief;
    }

    public String getErrorDetail() {
        return errorDetail;
    }


}
