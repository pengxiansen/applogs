package com.pengzhaopeng.comon;

import java.io.Serializable;

/**
 * 启动日志
 */
public class StartupReportLogs extends BasicLog {

    private String appVersion;
    private Long startTimeInMs;
    private Long activeTimeInMs;
    private String city;
    private int type;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public Long getStartTimeInMs() {
        return startTimeInMs;
    }

    public void setStartTimeInMs(Long startTimeInMs) {
        this.startTimeInMs = startTimeInMs;
    }

    public Long getActiveTimeInMs() {
        return activeTimeInMs;
    }

    public void setActiveTimeInMs(Long activeTimeInMs) {
        this.activeTimeInMs = activeTimeInMs;
    }
}
