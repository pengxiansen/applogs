package com.pengzhaopeng.comon;

import java.io.Serializable;

/**
 * BasicLog
 */
public class BasicLog implements Serializable {

    //0-启动  1-跳转  2-错误
    public static final int STARTUP_REPORT_LOGS = 0;
    public static final int PAGEVISIT_REPORT_LOGS = 1;
    public static final int ERROR_REPORT_LOGS = 2;

    private String userId;
    private String appId;
    private String appPlatform;


    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppPlatform() {
        return appPlatform;
    }

    public void setAppPlatform(String appPlatform) {
        this.appPlatform = appPlatform;
    }
}
