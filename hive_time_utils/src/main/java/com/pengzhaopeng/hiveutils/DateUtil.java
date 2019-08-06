package com.pengzhaopeng.hiveutils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    /**
     * 得到指定date的零时刻.
     */
    public static Date getDayBeginTime(Date d) {

        try {
            // 通过SimpleDateFormat获得指定日期的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");

            return sdf.parse(sdf.format(d));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到指定date的偏移量零时刻.
     * offset 偏移天数
     */
    public static Date getDayBeginTime(Date d, int offset) {

        try {
            // 通过SimpleDateFormat获得指定日期的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd 00:00:00");
            Date beginDate = sdf.parse(sdf.format(d));

            Calendar c = Calendar.getInstance();
            c.setTime(beginDate);
            // 通过Calendar实例实现按照天数偏移
            c.add(Calendar.DAY_OF_MONTH, offset);

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到指定date所在周的起始时刻.
     */
    public static Date getWeekBeginTime(Date d) {

        try {
            //得到指定日期d的零时刻
            Date beginDate = getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            assert beginDate != null;
            c.setTime(beginDate);
            //得到当前日期是所在周的第几天
            int n = c.get(Calendar.DAY_OF_WEEK);
            //通过减去周偏移量获得本周的第一天
            c.add(Calendar.DAY_OF_MONTH, -(n - 1));

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到距离指定date所在周offset周之后的一周的起始时刻.
     * offset 按周偏移
     */
    public static Date getWeekBeginTime(Date d, int offset) {

        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
            Calendar c = Calendar.getInstance();
            assert beginDate != null;
            c.setTime(beginDate);
            int n = c.get(Calendar.DAY_OF_WEEK);

            //定位到本周第一天
            c.add(Calendar.DAY_OF_MONTH, -(n - 1));
            //通过Calendar实现按周进行偏移
            c.add(Calendar.DAY_OF_MONTH, offset * 7);

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到指定date所在月的起始时刻.
     */
    public static Date getMonthBeginTime(Date d) {

        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
            //得到date所在月的第一天的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

            return sdf.parse(sdf.format(beginDate));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 得到距离指定date所在月offset个月之后的月的起始时刻.
     * offset 按月偏移
     */
    public static Date getMonthBeginTime(Date d, int offset) {

        try {
            //得到d的零时刻
            Date beginDate = getDayBeginTime(d);
            //得到date所在月的第一天的零时刻
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/01 00:00:00");

            //d所在月的第一天的零时刻
            Date firstDay = sdf.parse(sdf.format(beginDate));

            Calendar c = Calendar.getInstance();
            c.setTime(firstDay);

            //通过Calendar实现按月进行偏移
            c.add(Calendar.MONTH, offset);

            return c.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}
