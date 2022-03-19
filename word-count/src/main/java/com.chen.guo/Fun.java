package com.chen.guo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.util.Date;

public class Fun {
    public static void main(String[] args) throws ParseException {

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .appendZoneText(TextStyle.FULL)
                .toFormatter();

        String input = "2022-01-28 19:11:01.736788456+00:00";
        //String input = "2022-01-28 19:11:01.736788456 CST";
        // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

        LocalDateTime localDateTime = LocalDateTime.parse(input, formatter);
        Instant instant = localDateTime.atZone(ZoneId.of("+00:00")).toInstant();
        System.out.println(instant.toEpochMilli());  //1643397061736


        ZonedDateTime zdt = ZonedDateTime.parse(input, formatter);
        System.out.println(zdt.toInstant().toEpochMilli());


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setLenient(false);
        System.out.println(sdf.parse("2022-11-28 17:10:33").getTime());
    }
}
