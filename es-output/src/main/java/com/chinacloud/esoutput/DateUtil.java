package com.chinacloud.esoutput;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

/**
 * <p>DateUtil</p>
 * <p>Description:日期时间工具类</p>
 * @author xuwl fengsl 2013
 * @version 1.0
 */

public class DateUtil {

	 /**
     * 私有构造方法，防止类的实例化，因为工具类不需要实例化。
     */
    private DateUtil() {
    	
    }
   

    /**
     * 计算相隔一定时间间隔的时间。需要指定时间、间隔、以及间隔的时间单位
     * @param basicDate 基础时间
     * @param interval 间隔，整数
     * @param calendarType Calendar中定义的时间单位（Calendar.MINUTE、Calendar.HOUR、 Calendar.DATE、Calendar.MONTH）
     * @return 计算后的时间
     */
    public static Date addDateWithCalendar(Date basicDate, int interval, int calendarType) {
        // Calendar类可以实现此功能。
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(basicDate);
        calendar.add(calendarType, interval);
        return calendar.getTime();
    }
    
    /**
     * 取得  addLong 毫秒以前（以后）的时间
     * @param olddate 初始时间
     * @param addLong 增加的毫秒数
     * @return 更新后的时间
     */
    public static Date getAddDate(Date olddate, long addLong) {
        long temp = olddate.getTime();
        temp += addLong;
        return new Date(temp);
    }

    /**
     * 判断一个字符串是否为指定格式的时间
     * @param dateStr 字符串
     * @param pattern 时间样式
     * @return true/false
     */
    public static boolean isDate(String dateStr, String pattern) {
        SimpleDateFormat b = new SimpleDateFormat(pattern);
        try {
            b.parse(dateStr.trim());
            return true;
        } catch (ParseException ex) {
            return false;
        }
    }

    /**
     * 判断一个字符串是否为缺省格式的时间
     * "yyyy-MM-dd"或者"yyyy-MM-dd HH:mm:ss"格式
     * @param dateStr 字符串
     * @return true/false
     */
    public static boolean isDate(String dateStr) {
        String pattern = "yyyy-MM-dd";
        if (dateStr.indexOf(":") > 0) pattern = "yyyy-MM-dd HH:mm:ss";
        return isDate(dateStr, pattern);
    }

    /**
     * 判断一个字符串是否为"yyyy-MM-dd"格式的日期
     * @param dateStr 字符串
     * @return true/false
     */
    public static boolean isDateOnly(String dateStr) {
        String pattern = "yyyy-MM-dd";
        return isDate(dateStr, pattern);
    }

    /**
     * 判断一个字符串是否为"HH:mm:ss"格式的时间
     * @param dateStr 字符串
     * @return true/false
     */
    public static boolean isTimeOnly(String dateStr) {
        String pattern = "HH:mm:ss";
        return isDate(dateStr, pattern);
    }

    /**
     * 判断一个字符串是否为"yyyy-MM-dd HH:mm:ss"格式的时间
     * @param dateStr 字符串
     * @return true/false
     */
    public static boolean isDateTime(String dateStr) {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        return isDate(dateStr, pattern);
    }

    /**
     * 将指定模式的时间字符串转换为时间对象
     * @param dateStr 字符串
     * @param pattern 时间样式
     * @return Date，失败，则返回NULL。
     */
    public static Date getDate(String dateStr, String pattern) {
        if (dateStr==null || dateStr.equals("")) return null;
        SimpleDateFormat b = new SimpleDateFormat(pattern);
        try {
            return b.parse(dateStr.trim());
        } catch (ParseException ex) {
            return null;
        }
    }

    /**
     * 将缺省模式的时间字符串转换为时间对象
     * 可支持的日期类型有：yyyy-MM-dd HH:mm:ss，yyyy-MM-dd，yyyy年MM月dd日
     * 英文形式还可支持yyyy-MM-dd HH:mm，yyyy-MM-dd HH
     * @param dateStr 字符串
     * @return Date，失败，则返回NULL。
     */
    public static Date getDate(String dateStr) {
        Date temp1 = null;
        if (dateStr == null) return null;
        if (dateStr.equals("")) return null;
        SimpleDateFormat formatter = null;
        try {
            if (dateStr.indexOf(" ") != -1) {
                String[] aa = dateStr.split(":");
                if (aa.length == 3)
                    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                else if (aa.length == 2)
                    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                else
                    formatter = new SimpleDateFormat("yyyy-MM-dd HH");
            } else if(dateStr.indexOf("年") != -1){
                    formatter = new SimpleDateFormat("yyyy年MM月dd日");
            } else{
                formatter = new SimpleDateFormat("yyyy-MM-dd");
            }
            temp1 = formatter.parse(dateStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return temp1;
    }
    
    /**
     * 根据毫秒数返回Date格式日期
     * @param timestamp 日期毫秒数
     * @return 指定毫秒数的日期
     */
    public static Date getDate(long timestamp) {
    	return new Date(timestamp);
    }
    
    /**
     * 把Object对像转换成Date类型
     * 如果对像为空或格式不能解析，返回当前日期
     * @param o
     * @return Date对象
     */
    public static Date getDate(Object o) {
        if (o == null) {
            return new Date();
        } else if (o instanceof Date) {
            return (Date) o;
        } else if (o instanceof String) {
            return getDate(String.valueOf(o));
        } else if (o instanceof java.sql.Timestamp) {
            return new Date(((java.sql.Timestamp) o).getTime());
        } else {
            return new Date();
        }
    }

    /**
     * 将缺省模式的时间字符串转换为yyyy-MM-dd格式的时间对象
     * @param dateStr 字符串
     * @return Date，失败，则返回NULL。
     */
    public static Date getDateOnly(String dateStr) {
        if (dateStr==null || dateStr.equals("")) return null;
        String pattern = "yyyy-MM-dd";
        return getDate(dateStr, pattern);
    }

    /**
     * 将缺省模式的时间字符串转换为HH:mm:ss格式的时间对象
     * @param dateStr 字符串
     * @return Date，失败，则返回NULL。
     */
    public static Date getTimeOnly(String dateStr) {
        if (dateStr==null || dateStr.equals("")) return null;
        String pattern = "HH:mm:ss";
        return getDate(dateStr, pattern);
    }

    /**
     * 将缺省模式的时间字符串转换为yyyy-MM-dd HH:mm:ss格式的时间对象
     * @param dateStr 字符串
     * @return Date，失败，则返回NULL。
     */
    public static Date getDateTime(String dateStr) {
        if (dateStr==null || dateStr.equals("")) return null;
        String pattern = "yyyy-MM-dd HH:mm:ss";
        return getDate(dateStr, pattern);
    }

    /**
     * 返回指定模式（pattern）的格式的日期字符串。
     * @param date Date 指定日期（包括时间）
     * @param pattern String 模式 "yyyy.MM.dd G 'at' HH:mm:ss z" 2001.07.04 AD at 12:08:56 PDT
     * @return String 指定格式的时间字符串
     */
    public static String getDateString(Date date, String pattern) {
        if (date == null) return "";
        DateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(date);
    }

    /**
     * 返回指定模式（pattern）的格式的今天的日期字符串。"yyyy.MM.dd G 'at' HH:mm:ss z" 2001.07.04 AD at 12:08:56 PDT
     * @param pattern String 模式 "yyyy.MM.dd G 'at' HH:mm:ss z" 2001.07.04 AD at 12:08:56 PDT
     * @return String 指定格式的时间字符串
     */
    public static String getDateString(String pattern) {
        return getDateString(new Date(), pattern);
    }

    /**
     * 返回日期时间对应的含时间的字符串，形为：yyyy-MM-dd HH:mm:ss
     * @param date Date 日期对象
     * @return String 形为：yyyy-MM-dd HH:mm:ss 的字符串
     */
    public static String getDateTimeString(Date date) {
    	if (date == null) return "";
        return getDateString(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 返回当前日期时间对应的含时间的字符串，形为：yyyy-MM-dd HH:mm:ss
     * @return String 形为：yyyy-MM-dd HH:mm:ss 的字符串
     */
    public static String getDateTimeString() {
        return getDateString("yyyy-MM-dd HH:mm:ss");
    }
    
    /**
     * 日期格式化成日期时分，不取秒，
     * @param date
     * @return String 形为：2005-12-25 12:25
     */
    public static String getDateHF(Date date) {
        if (date == null) return "";
        return  getDateString(date, "yyyy-MM-dd HH:mm");
    }

    /**
     * 返回日期对应的字符串，形为：yyyy-MM-dd
     * @param date Date 日期对象
     * @return String 形为：yyyy-MM-dd 的字符串
     */
    public static String getDateString(Date date) {
        return getDateString(date, "yyyy-MM-dd");
    }

    /**
     * 返回当前日期对应的字符串，形为：yyyy-MM-dd
     * @return String 形为：yyyy-MM-dd 的字符串
     */
    public static String getDateString() {
        return getDateString("yyyy-MM-dd");
    }

    /**
     * 返回日期时间对应的仅有时间的字符串，形为：HH:mm:ss
     * @param date Date 日期对象
     * @return String 形为：HH:mm:ss 的字符串
     */
    public static String getTimeString(Date date) {
        return getDateString(date, "HH:mm:ss");
    }

    /**
     * 返回当前日期时间对应的仅有时间的字符串，形为：HH:mm:ss
     * @return String 形为：HH:mm:ss 的字符串
     */
    public static String getTimeString() {
        return getDateString("HH:mm:ss");
    }
    
    /**
     * 返回当前日期时间的Timestamp对象,格式为:yyyy-MM-dd HH:mm:ss
     * @return Timestamp
     */
    public static java.sql.Timestamp getTimestamp() throws Exception {
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        java.util.Calendar calendar = java.util.Calendar.getInstance();
        String mystrdate = myFormat.format(calendar.getTime());
        return java.sql.Timestamp.valueOf(mystrdate);
    }

    /**
     * 返回指定日期时间字符串的日期时间Timestamp对象,格式为:yyyy-MM-dd HH:mm:ss
     * @param datestr String 日期时间字符串,格式为:yyyy-MM-dd HH:mm:ss
     * @return Timestamp
     */
    public static java.sql.Timestamp getTimestamp(String datestr) throws Exception {
        if (datestr==null || datestr.equals("")) return null;
        if (datestr.indexOf(':') < 0) { // 没有找到时间项
            datestr = datestr.trim() + " 00:00:00";
        }
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String mystrdate = myFormat.format(myFormat.parse(datestr.trim()));
        return java.sql.Timestamp.valueOf(mystrdate);
    }

    /**
     * 返回指定日期字符串的日期Timestamp对象,格式为:yyyy-MM-dd
     * @param datestr String 日期字符串,格式为:yyyy-MM-dd
     * @return Timestamp
     */
    public static java.sql.Timestamp getTimestampDate(String datestr) throws Exception {
        if (datestr==null || datestr.equals("")) return null;
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = myFormat.parse(datestr.trim());
        return new java.sql.Timestamp(date.getTime());
    }

    /**
     * 返回指定日期时间Timestamp对象的对应字符串表达式,格式为:yyyy-MM-dd HH:mm:ss
     * @param java.sql.Timestamp 日期时间Timestamp对象
     * @return String 指定日期时间Timestamp对象的对应字符串表达式
     */
    public static String getTimestampString(java.sql.Timestamp tst) throws Exception {
        if (tst == null) return "";
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(tst.getTime());
        return myFormat.format(date);
    }

    /**
     * 返回指定日期时间Timestamp对象的对应日期字符串表达式,格式为:yyyy-MM-dd
     * @param java.sql.Timestamp 日期时间Timestamp对象
     * @return String 指定日期时间Timestamp对象的对应字符串表达式
     */
    public static String getTimestampDateString(java.sql.Timestamp tst) throws Exception {
        if (tst == null) return "";
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(tst.getTime());
        return myFormat.format(date);
    }

    /**
     * 返回指定日期时间Timestamp对象的对应时间字符串表达式,格式为:HH:mm:ss
     * @param java.sql.Timestamp 日期时间Timestamp对象
     * @return String 指定日期时间Timestamp对象的对应字符串表达式
     */
    public static String getTimestampTimeString(java.sql.Timestamp tst) throws Exception {
        if (tst == null) return "";
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("HH:mm:ss");
        Date date = new Date(tst.getTime());
        return myFormat.format(date);
    }
    
    /**
     * 取得当前日期的中文格式的日期
     * @return 2005年12月25日
     */
    public static String getChineseDate() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy年MM月dd日");
        String NDate = formatter.format(new Date());
        return NDate;
    }
    /**
     * 取得指定日期的中文格式的日期带参数
     * @return 2009年09月18日，如果参数为null则返回""
     */
     public static String getChineseDate(Date date) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy年MM月dd日");
        String NDate="";
        if(date==null){
            return ""; 
        }else{
            NDate = formatter.format(date);
        }
        return NDate;
    }
    
     /**
      * 返回指定日期时间Date对象的对应日期中文字符串表达式,格式为:yyyy年MM月dd日 HH时mm分ss秒
      * @param Date 日期时间Date对象
      * @return String 指定日期时间Date对象的对应字符串表达式
      */
     public static String getChineseString(Date date) throws Exception {
    	 if (date == null) return "";
    	 java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
    	 return myFormat.format(date);
     }
     
    /**
     * 返回指定日期时间Timestamp对象的对应日期中文字符串表达式,格式为:yyyy年MM月dd日 HH时mm分ss秒
     * @param java.sql.Timestamp 日期时间Timestamp对象
     * @return String 指定日期时间Timestamp对象的对应字符串表达式
     */
    public static String getChineseString(Timestamp tst) throws Exception {
        if (tst == null) return "";
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");
        Date date = new Date(tst.getTime());
        return myFormat.format(date);
    }
    
    /**
     * 返回指定日期时间Timestamp对象的对应日期中文字符串表达式,格式为:yyyy年MM月dd日
     * @param java.sql.Timestamp 日期时间Timestamp对象
     * @return String 指定日期时间Timestamp对象的对应字符串表达式
     */
    public static String getChineseDate(Timestamp tst) throws Exception {
        if (tst == null) return "";
        java.text.SimpleDateFormat myFormat = new SimpleDateFormat("yyyy年MM月dd日");
        Date date = new Date(tst.getTime());
        return myFormat.format(date);
    }

    /**
     * 比较两个Timestamp日期时间对象是否相等
     * @param source 日期时间Timestamp对象
     * @param target 日期时间Timestamp对象
     * @return boolean true:两日期时间对象相等;false:两日期时间对象不等
     */
    public static boolean compareTimestamp(java.sql.Timestamp source, java.sql.Timestamp target)
            throws Exception {
        if (source.compareTo(target) == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 比较Timestamp日期时间对象和一个日期格式字符串所代表的日期时间是否相等
     * @param source 日期时间Timestamp对象
     * @param target 日期字符串,格式为:yyyy-MM-dd
     * @return boolean true:两日期时间相等;false:两日期时间不等
     */
    public static boolean compareTimestamp(java.sql.Timestamp source, String target)
            throws Exception {
        if (source.compareTo(getTimestampDate(target)) == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 比较两个日期格式字符串所代表的日期时间是否相等
     * @param source 日期字符串,格式为:yyyy-MM-dd
     * @param target 日期字符串,格式为:yyyy-MM-dd
     * @return boolean true:两日期时间相等;false:两日期时间不等
     */
    public static boolean compareTimestamp(String source, String target) throws Exception {
        if (getTimestampDate(source).compareTo(getTimestampDate(target)) == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 比较并取得两个Timestamp日期时间对象相差天数
     * @param source 日期时间Timestamp对象
     * @param target 日期时间Timestamp对象
     * @return long 两日期时间对象相差天数
     */
    public static long compareTimestampDay(java.sql.Timestamp source, java.sql.Timestamp target)
            throws Exception {
        return ((source.getTime() - target.getTime()) / 3600 / 24 / 1000);
    }

    /**
     *  比较并取得一个Timestamp日期时间对象和一个日期格式的字符串所代表的日期时间相差天数
     * @param source 日期时间Timestamp对象
     * @param target 日期字符串,格式为:yyyy-MM-dd
     * @return long 两日期时间相差天数
     */
    public static long compareTimestampDay(java.sql.Timestamp source, String target)
            throws Exception {
        return ((source.getTime() - getTimestampDate(target).getTime()) / 3600 / 24 / 1000);
    }

    /**
     *  比较并取得两个日期格式的字符串所代表的日期时间相差天数
     * @param source 日期字符串,格式为:yyyy-MM-dd
     * @param target 日期字符串,格式为:yyyy-MM-dd
     * @return long 两日期时间相差天数
     */
    public static long compareTimestampDay(String source, String target) throws Exception {
        return ((getTimestampDate(source).getTime() - getTimestampDate(target).getTime()) / 3600 / 24 / 1000);
    }
    
    /**
     * 比较两个yyyy-MM-dd HH:mm:ss格式的字符串日期大小
     * @param last 日期1
     * @param now 日期2
     * @return 如果last比now小则返回true，否则返回false
     */
    public static boolean compareTo(String last, String now) {
        try {
            Date temp1 = getDate(last);
            Date temp2 = getDate(now);
            if (temp1.after(temp2)) {
                return false;
            } else if (temp1.before(temp2)) {
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    
    /**
     * 获取两个字符串类型日期之间相差的天数
     * @param date1
     * @param date2
     * @return 相差天数的绝对值
     */
    public static int DateDiff(String date1, String date2) {
        long d1 = Math.abs(getDate(date2).getTime() - getDate(date1).getTime());
        float j= d1 / 1000 / 60 / 60 / 24;
        return Math.round(j);
    }
    
    /**
     * 获取两个日期之间相差的天数
     * @param date1
     * @param date2
     * @return  相差天数的绝对值
     */
    public static int DateDiff(Date date1, Date date2) {
        if (date1 == null || date2 == null) return 0;
        long d1 = Math.abs(date2.getTime() - date1.getTime());
        float j= d1 / 1000 / 60 / 60 / 24;
        return Math.round(j);
    }
    
    /**
     * 根据指定Date时间取得Timestamp类型的时间
     * @param dateTime
     * @return Timestamp对象
     */
    public static Timestamp getTime(Date date){
    	return (new Timestamp(date.getTime()));
    }

    /**
     * 取得当月天数
     * @return 取得当月天数
     */
    public static int getCurrentMonthLastDay() {
        Calendar a = Calendar.getInstance();
        a.set(Calendar.DATE, 1); // 把日期设置为当月第一天
        a.roll(Calendar.DATE, -1); // 日期回滚一天，也就是最后一天
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }

    /**
     * 得到指定月的天数
     * @param year 指定年yyyy
     * @param month 指定月M
     * @return 得到指定月的天数
     */
    public static int getMonthLastDay(int year, int month) {
        Calendar a = Calendar.getInstance();
        a.set(Calendar.YEAR, year);
        a.set(Calendar.MONTH, month - 1);
        a.set(Calendar.DATE, 1); // 把日期设置为当月第一天
        a.roll(Calendar.DATE, -1); // 日期回滚一天，也就是最后一天
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }
    
    /**
     * 将util.date转成sql.date
     * @param d  util.date对象
     * @return SQL类型的DATE
     */
    public static java.sql.Date getSQLDate(Date d) {
        return new java.sql.Date(d.getTime());
    }

    /**
     * 把long型日期转换成天数
     * @param date
     * @return 天数的字符串
     */
    public static String getDateLength(long date) {
        String s = "";
        if (date > 1000 * 60 * 60 * 24) {
            s += date / (1000 * 60 * 60 * 24) + "天";
            date = date % (1000 * 60 * 60 * 24);
        }
        if (date > 1000 * 60 * 60) {
            s += date / (1000 * 60 * 60) + "时";
            date = date % (1000 * 60 * 60);
        }
        if (date > 1000 * 60) {
            s += date / (1000 * 60) + "分";
        }
        return s;
    }


    /**
     * 取得当前星期几
     * @return 星期几
     */
    public static String getWeekDay() {
        String[] weekDay = new String[]{"星期日", "星期一", "星期二",
                "星期三", "星期四", "星期五", "星期六"};
        Calendar ca = Calendar.getInstance();
        return weekDay[ca.get(Calendar.DAY_OF_WEEK) - 1];
    }

    /**
     * 取得指定周开始日期
     * 默认从该周周一开始
     *
     * @param year
     * @param week
     * @return 该周第一天
     */
    public static Date getWeekBeginDate(int year, int week) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.WEEK_OF_YEAR, week);
        c.set(Calendar.DAY_OF_WEEK, 2);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return c.getTime();
    }

    /**
     * 取得当前周的开始日期
     * 默认从该周周一开始
     *
     * @return 当前周第一天
     */
    public static Date getWeekBeginDate() {
        Date d = new Date();
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, d.getYear() + 1900);
        c.set(Calendar.WEEK_OF_YEAR, getWeek(d));
        c.set(Calendar.DAY_OF_WEEK, 2);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return c.getTime();
    }
    
    /**
     * 取得指定周结束日期，默认为该周周日
     * @param year
     * @param week
     * @return 结束日期
     */
    public static Date getWeekEndDate(int year, int week) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.WEEK_OF_YEAR, week + 1);
        c.set(Calendar.DAY_OF_WEEK, 1);
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return c.getTime();
    }

    /**
     * 取得当前周的结束日期
     * 默认为该周周日
     * @return 结束日期
     */
    public static Date getWeekEndDate() {
        Date d = new Date();
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, d.getYear() + 1900);
        c.set(Calendar.WEEK_OF_YEAR, getWeek(d) + 1);
        c.set(Calendar.DAY_OF_WEEK, 1);
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return c.getTime();
    }

    /**
     * 取得指定月的开始日期
     * @param year
     * @param month
     * @return 该月第一天
     */
    public static Date getMonthBeginDate(int year, int month) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month - 1);
        c.set(Calendar.DAY_OF_MONTH, 1);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        return c.getTime();
    }

    /**
     * 取得当前月开始日期
     * @return 当前月第一天
     */
    public static Date getMonthBeginDate() {
    	Calendar calendar = Calendar.getInstance();
 	    calendar.set(Calendar.DAY_OF_MONTH, calendar
 	            .getActualMinimum(Calendar.DAY_OF_MONTH));
 	    return getDate(calendar.getTimeInMillis());
    }

    /**
     * 取得指定年月的结束日期
     * @param year
     * @param month
     * @return 指定年月最后一天
     */
    public static Date getMonthEndDate(int year, int month) {
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, year);
        c.set(Calendar.MONTH, month);
        c.set(Calendar.DAY_OF_MONTH, 0);
        c.set(Calendar.HOUR_OF_DAY, 23);
        c.set(Calendar.MINUTE, 59);
        c.set(Calendar.SECOND, 59);
        return c.getTime();
    }
    
    /**
     * 取得当前月结束日期
     * @return 当前月最后一天
     */
    public static Date getMonthEndDate() {
    	Calendar calendar = Calendar.getInstance();
	    calendar.set(Calendar.DAY_OF_MONTH, calendar
	            .getActualMaximum(Calendar.DAY_OF_MONTH));
	    return getDate(calendar.getTimeInMillis());
    }

    /**
     * 取得指定日期的周数
     * @param d
     * @return 周数
     */
    public static int getWeek(Date d) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        return c.get(Calendar.WEEK_OF_YEAR);
    }
	
	
	/**
     * 根据毫秒数返回yyyy-MM-dd格式日期
     * @param timestamp 日期
     * @return 格式化后的日期，格式如：2005-12-04
     */
    public static String convertDate(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp));
    }
    
	/**
	 * 获取指定日期的毫秒
	 * @param p_date util.Date日期
	 * @return long   毫秒
	 * @Date:   2006-10-31
	*/
	public static long getMillisOfDate(Date p_date ) {
	   java.util.Calendar c = java.util.Calendar.getInstance();
	   c.setTime( p_date );
	   return c.getTimeInMillis();
	}
	
    /**
     * 取得当前日期的Long型，即毫秒数
     * 
     * @return 当前日期的毫秒数，如1212452121222
     */
    public static long getTime() {
        return (new java.util.Date()).getTime();
    }
    
    /**
     * 获取指定日期的日期时间.毫秒格式字符串
     * @param date 传入的日期
     * @return 形如：2013-12-17 14:13:50.878 格式的字符串
     */
    public static String getDateTimeMicroString(Date date){
        long time = date.getTime();
        return getDateTimeString(date) + "." + (time - time / 1000 * 1000);
    }
    
    /**
     * 获取当前日期的日期时间.毫秒格式字符串
     * @param date 传入的日期
     * @return 形如：2013-12-17 14:13:50.878 格式的字符串
     */
    public static String getDateTimeMicroString(){
        return getDateTimeMicroString(new Date());
    }

    /**
     * 给定日期 返回String数组,数组元素为,年,月,日,时,分,秒
     * @param stime 日期 格式形如:2011-04-01 10:12:12
     * @return 指定日期的年,月,日,时,分,秒数组
     * @throws Exception
     */
    public static ArrayList<String> getStringYMDHMS(String stime) throws Exception {
        if (stime == null) stime = "";
        if (stime.indexOf(" ") <= 0 || stime.equals("")) throw new Exception("参数错误");
        String[] temp1 = stime.split(" ");
        if (temp1[0] == null || temp1[1] == null) return null;
        if (temp1[0].indexOf("-") <= 0 || temp1[1].indexOf(":") <= 0) return null;
        ArrayList<String> al = new ArrayList<String>();
        String[] temp11 = temp1[0].split("-");
        al.add(temp11[0]);//年
        al.add(temp11[1]);//月
        al.add(temp11[2]);//日
        String[] temp22 = temp1[1].split(":");
        al.add(temp22[0]);//时
        al.add(temp22[1]);//分
        al.add(temp22[2]);//秒
        return al;
    }

    /**
     * 给定日期 返回int数组,数组元素为,年,月,日,时,分,秒
     * @param stime 日期 格式形如:2011-04-01 10:12:12
     * @return 年,月,日,时,分,秒数组的int值
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public static ArrayList getIntYMDHMS(String stime) throws Exception {
    	 if (stime == null) stime = "";
        if (stime.indexOf(" ") <= 0 || stime.equals("")) throw new Exception("参数错误");
        String[] temp1 = stime.split(" ");
        if (temp1[0] == null || temp1[1] == null) return null;
        if (temp1[0].indexOf("-") <= 0 || temp1[1].indexOf(":") <= 0) return null;
        ArrayList al = new ArrayList();
        String[] temp11 = temp1[0].split("-");
        al.add(Integer.parseInt(temp11[0]));//年
        al.add(Integer.parseInt(temp11[1]));//月
        al.add(Integer.parseInt(temp11[2]));//日
        String[] temp22 = temp1[1].split(":");
        al.add(Integer.parseInt(temp22[0]));//时
        al.add(Integer.parseInt(temp22[1]));//分
        al.add(Integer.parseInt(temp22[2]));//秒
        return al;
    }
    
    /**
	 * 通过字符串格式时间返回Calendar格式时间
	 * @param sdate	字符串时间格式 形如:2011-03-11
	 * @return Calendar格式时间
	 */
	public static Calendar getCalendarDateByString(String sdate){
		Calendar startDateForDay = Calendar.getInstance();
		startDateForDay.setTime(DateUtil.getDate(sdate));
		return startDateForDay;
	}
	
	/**
     * 时间比较:判断target是否晚于soure
     * @param soure 源日期 形如:2011-04-01
     * @param target 目标日期 如:2011-05-01
     * @return 如果是返回true,否则返回false
     */
    public static boolean ifAfter(String soure, String target) {
        Calendar sourceCalendar = Calendar.getInstance();
        Calendar targetCalendar = Calendar.getInstance();
        targetCalendar.setTime(getDate(target));// 只保留年月日的日期，时分秒设为0
        sourceCalendar.setTime(DateUtil.getDate(soure));// 只保留年月日的日期，时分秒设为0
        if (sourceCalendar.compareTo(targetCalendar) > 0)
            return true;
        else
            return false;
    }

    /**
     * 时间比较:判断target是否晚于soure 包含时分秒
     * @param soure 源日期 形如:2011-04-01 10:12:12
     * @param target 目标日期 如:2011-05-01 11:12:25
     * @return 如果是返回true,否则返回false
     */
    public static boolean ifAfterContainHMS(String soure, String target) {
        Calendar sourceCalendar = Calendar.getInstance();
        Calendar targetCalendar = Calendar.getInstance();
        targetCalendar.setTime(DateUtil.getDateTime(target));// 只保留年月日的日期，时分秒设为0
        sourceCalendar.setTime(DateUtil.getDateTime(soure));// 只保留年月日的日期，时分秒设为0
        if (sourceCalendar.compareTo(targetCalendar) > 0)
            return true;
        else
            return false;
    }
    
    /**
	 * 计算时间差（天数）
	 * 
	 * @param startTime
	 * @param endTime
	 * @return 日期的天数差
	 */

	public static long diffMinDays(Date startTime, Date endTime)
			throws Exception {
		// TODO Auto-generated method stub
		long sumTime = endTime.getTime() - startTime.getTime();
		long days = (sumTime) / (1000 * 60 * 60 * 24);
		return days;
	}

	/**
	 * 计算天差（天份数）
	 * 
	 * @param endTime
	 * @param startTime
	 * @return 日期的天差
	 * @throws Exception
	 */
	public static int diffDays(String startTime, String endTime)
			throws Exception {
		// java.util.Date nowTime = new java.util.Date();
		java.text.SimpleDateFormat sf = new java.text.SimpleDateFormat(
				"yyyy-MM-dd");
		java.util.Date b = sf.parse(endTime);
		java.util.Date d = sf.parse(startTime);
		long c = b.getDate() - d.getDate();
		return (int) c;
	}

	/**
	 * 计算月份差（月份数）
	 * 
	 * @param endTime
	 * @param startTime
	 * @return 日期的月份差
	 * @throws Exception
	 */
	public static int diffMon(String startTime, String endTime)
			throws Exception {
		// java.util.Date nowTime = new java.util.Date();
		java.text.SimpleDateFormat sf = new java.text.SimpleDateFormat(
				"yyyy-MM-dd");
		java.util.Date b = sf.parse(endTime);
		java.util.Date d = sf.parse(startTime);
		long c = b.getMonth() - d.getMonth();
		return (int) c;
	}

	/**
	 * 计算年份差（年份数）
	 * 
	 * @param endTime
	 * @param startTime
	 * @return 日期的年份差
	 * @throws Exception
	 */
	public static int diffYear(String startTime, String endTime)
			throws Exception {
		// java.util.Date nowTime = new java.util.Date();
		java.text.SimpleDateFormat sf = new java.text.SimpleDateFormat(
				"yyyy-MM-dd");
		java.util.Date b = sf.parse(endTime);
		java.util.Date d = sf.parse(startTime);
		long c = b.getYear() - d.getYear();
		return (int) c;
	}

	/**
	 * 计算生日，精确到天计算
	 * 
	 * @param startTime
	 * @param endTime
	 * @return 年龄
	 * @throws Exception
	 */
	public static int getBirthday(String startTime, String endTime)
			throws Exception {
		int birthday = diffYear(startTime, endTime);
		if (birthday <= 0) {
			return 0;
		}
		if (diffMon(startTime, endTime) < 0) {
			return birthday - 1;
		} else if (diffMon(startTime, endTime) > 0) {
			return birthday;
		} else {
			if (diffDays(startTime, endTime) >= 0) {
				return birthday;
			} else if (diffDays(startTime, endTime) < 0) {
				return birthday - 1;
			}
		}
		return 0;
	}
	
    /**
     * 判断某年是否是闰年
     * @param year int型整数
     * @return 闰年：true，非闰年：false
     * @throws Exception
     */
 	public static boolean isRunNian (int year) {
 		boolean flag = false;   
 		if (year%4 == 0 || (year%100 == 0 && year%400 != 0)) { 
 			flag = true;  
 		}    
 		return flag; 
 	}
 	
 	 /**
     * 获取当前系统时间
     * @return 返回Date类型的当前系统时间
     */
    public static Date getCurrentDate() {
        return new Date(System.currentTimeMillis());
    }
    
    /**
     * 根据日期生成目录
     * @param d
     * @return 形如：/2013/12/12
     */
    public static String getDatePath(Date d) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        StringBuffer path = new StringBuffer();
        path.append("/");
        path.append(c.get(Calendar.YEAR));
        path.append("/");
        path.append(c.get(Calendar.MONTH) + 1);
        path.append("/");
        path.append(c.get(Calendar.DATE));
        return path.toString();
    }
    
    /**
     * 获取月日的大写名称
     * @param source 月日字符串 
     * @return 两位月(日)的大写名称
     */
    public static String retString(String source) {
        String chends = "";
        String[] num = {
                "", "一", "二", "三", "四", "五", "六", "七", "八", "九"};
        int ii = Integer.parseInt(source);
        if (ii > 9){
        	if(source.charAt(0)=='1')
        		chends="十";
        	if(source.charAt(0)=='2')
        		chends="二十";
        	if(source.charAt(0)=='3')
        		chends="三十";
        	chends += num[Integer.parseInt(String.valueOf(source.charAt(1)))];
        }else {
        	chends = num[Integer.parseInt(String.valueOf(source.charAt(1)))];
		}
        return chends;
    }
    
    /**
     * 将字符串日期转为大写的中文名称
     * @param str 日期字符串，可以是“-”或者“/”分隔，也可以包含时分秒
     * @return 转换后的字符串，如：二〇一三年十二月十三日
     */
    public static String getUpperStrDate(String str) {
        // String str =
        // Tools.getDateString(Tools.getDate(Tools.getDateString()),"yyyy/MM/dd");
        String[] num = {
                "〇", "一", "二", "三", "四", "五", "六", "七", "八", "九"};
        String year = str.substring(0, 4);
        String month = str.substring(5, 7);
        String day = str.substring(8,10);
        String chend = "";
        for (int i = 0; i < 4; i++) {
            chend+=num[Integer.parseInt(String.valueOf(year.charAt(i)))];
        }
        chend +="年";
        chend += retString(month) + "月";
        chend += retString(day) + "日";
        return chend;
    }
    
    /**
     * 将字符串日期转为大写的中文名称
     * @param date 日期对象
     * @return 转换后的字符串，如：二〇一三年十二月十三日
     */
    public static String getUpperStrDate(Date date) {
    	String str =getDateString(date);
    	return getUpperStrDate(str);
    }
    
    /**
     * 将当前日期转为大写的中文名称
     * 
     * @return 转换后的字符串，如：二〇一三年十二月十三日
     */
    public static String getUpperStrDate() {
    	String str =getDateString();
    	return getUpperStrDate(str);
    }
}
