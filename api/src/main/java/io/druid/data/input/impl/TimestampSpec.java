/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.metamx.common.parsers.TimestampParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class TimestampSpec
{
  private static class ParseCtx
  {
    Object lastTimeObject = null;
    DateTime lastDateTime = null;
  }

  private static final String DEFAULT_COLUMN = "timestamp";
  private static final String DEFAULT_FORMAT = "auto";
  private static final DateTime DEFAULT_MISSING_VALUE = null;

  //private final String timezone;
  private final String timezone;
  private final String timestampColumn;
  private final String timestampFormat;
  private final Function<Object, DateTime> timestampConverter;
  // this value should never be set for production data
  private final DateTime missingValue;

  // remember last value parsed
  private ParseCtx parseCtx = new ParseCtx();

  @JsonCreator
  public TimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      @JsonProperty("timezone") String timezone,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue
  )
  {
    this.timestampColumn = (timestampColumn == null) ? DEFAULT_COLUMN : timestampColumn;
    this.timestampFormat = format == null ? DEFAULT_FORMAT : format;
    this.timestampConverter = TimestampParser.createObjectTimestampParser(timestampFormat);
    this.missingValue = missingValue == null
                        ? DEFAULT_MISSING_VALUE
                        : missingValue;

    /**
     * 获取并解析　用户提交的时区
     */
    String Usertimezone = "+08:00";

    if(timezone != null && timezone.equals("UTC")){
      Usertimezone = timezone;
    }else if(timezone != null && timezone.startsWith("UTC")
            && timezone.substring(3).matches("^[-+]\\d{4}$")){
      Usertimezone = timezone.substring(3);
    }

    this.timezone = Usertimezone;

  }


  @JsonProperty("column")
  public String getTimestampColumn()
  {
    return timestampColumn;
  }

  @JsonProperty("format")
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  @JsonProperty("missingValue")
  public DateTime getMissingValue()
  {
    return missingValue;
  }

  public DateTime extractTimestamp(Map<String, Object> input)
  {

    // 保留之前的时区
    DateTimeZone timeZoneBak = DateTimeZone.getDefault();

    // 设置用户时区
    DateTimeZone.setDefault(DateTimeZone.forID(timezone));
    final Object o = input.get(timestampColumn);
    DateTime extracted = missingValue;
    if (o != null) {
      if (o.equals(parseCtx.lastTimeObject)) {
        extracted = parseCtx.lastDateTime;
      } else {
        ParseCtx newCtx = new ParseCtx();
        newCtx.lastTimeObject = o;
        extracted = timestampConverter.apply(o);

        // System.out.println("parse time -- " + extracted);
        /*
          * 需要将这个日期转换成的用户日期
           */
        /*
        if(timestampFormat.equals("posix") || timestampFormat.equals("millis") || timestampFormat.equals("nano")){   // 时间戳类型

          extracted = extracted.withZone(DateTimeZone.forID(timezone));
          extracted = extracted.withZoneRetainFields(DateTimeZone.getDefault());
        }

        System.out.println("end -- " + extracted);
        */

        newCtx.lastDateTime = extracted;
        parseCtx = newCtx;
      }
    }

    System.out.println("--------------start : " + extracted);
    // 转换成默认时区
    extracted = extracted.withZoneRetainFields(timeZoneBak);
    System.out.println("--------------end : " + extracted);
    DateTimeZone.setDefault(timeZoneBak);
    return extracted;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TimestampSpec that = (TimestampSpec) o;

    if (!timestampColumn.equals(that.timestampColumn)) {
      return false;
    }
    if (!timestampFormat.equals(that.timestampFormat)) {
      return false;
    }
    return !(missingValue != null ? !missingValue.equals(that.missingValue) : that.missingValue != null);

  }

  @Override
  public int hashCode()
  {
    int result = timestampColumn.hashCode();
    result = 31 * result + timestampFormat.hashCode();
    result = 31 * result + (missingValue != null ? missingValue.hashCode() : 0);
    return result;
  }

  //simple merge strategy on timestampSpec that checks if all are equal or else
  //returns null. this can be improved in future but is good enough for most use-cases.
  public static TimestampSpec mergeTimestampSpec(List<TimestampSpec> toMerge) {
    if (toMerge == null || toMerge.size() == 0) {
      return null;
    }

    TimestampSpec result = toMerge.get(0);
    for (int i = 1; i < toMerge.size(); i++) {
      if (toMerge.get(i) == null) {
        continue;
      }
      if (!Objects.equals(result, toMerge.get(i))) {
        return null;
      }
    }

    return result;
  }
}
