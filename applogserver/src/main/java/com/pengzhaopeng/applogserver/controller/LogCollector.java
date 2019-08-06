package com.pengzhaopeng.applogserver.controller;

import com.alibaba.fastjson.JSON;

import com.pengzhaopeng.comon.ErrorReportLogs;
import com.pengzhaopeng.comon.PageVisitReportLogs;
import com.pengzhaopeng.comon.StartupReportLogs;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Controller
@RequestMapping("/logs")
//@RestController == Responseody + controller
public class LogCollector {

    private static final Logger logger = LoggerFactory.getLogger(LogCollector.class);

    @PostMapping("/startupLogs")
    @ResponseBody
    public void startupReportLogs(@RequestBody StartupReportLogs startupReportLogs) {
        String json = JSON.toJSONString(startupReportLogs);
        System.out.println("APP 启动日志：" + json);

        // 写入日志目录
        logger.info(json);
    }

    @RequestMapping("/pageLogs")
    @ResponseBody
    public void pageCollect(@RequestBody PageVisitReportLogs pageVisitReportLogs){
        String json = JSON.toJSONString(pageVisitReportLogs);
        System.out.println("APP 页面跳转日志：" + json);

        // 写入日志目录
        logger.info(json);
    }

    @RequestMapping("/errorLogs")
    @ResponseBody
    public void errorCollect(@RequestBody ErrorReportLogs errorReportLogs){
        String json = JSON.toJSONString(errorReportLogs);
        System.out.println("APP 错误日志：" + json);

        // 写入日志目录
        logger.info(json);
    }
}
