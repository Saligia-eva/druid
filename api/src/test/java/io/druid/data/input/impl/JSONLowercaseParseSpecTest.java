/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input.impl;

import com.google.common.collect.Lists;
import com.metamx.common.parsers.JSONToLowerParser;
import com.metamx.common.parsers.Parser;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class JSONLowercaseParseSpecTest
{
  @Test
  public void testLowercasing() throws Exception
  {
    JSONLowercaseParseSpec spec = new JSONLowercaseParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
                null,
            null
        ),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(Arrays.asList("A", "B")),
            Lists.<String>newArrayList(),
            Lists.<SpatialDimensionSchema>newArrayList()
        )
    );
    Parser parser = spec.makeParser();
    Map<String, Object> event = parser.parse("{\"timestamp\":\"2015-01-01\",\"A\":\"foo\"}");
    Assert.assertEquals("foo", event.get("a"));
  }
}
