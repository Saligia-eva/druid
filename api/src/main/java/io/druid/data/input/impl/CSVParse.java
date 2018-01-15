package io.druid.data.input.impl;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.collect.Utils;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by saligia on 17-3-29.
 */
public class CSVParse implements Parser<String, Object> {
    private static final Logger log = new Logger(CSVParse.class);
    private final String listDelimiter;
    private final Splitter listSplitter;
    private final Function<String, Object> valueFunction;
    private final CSVParser parser;
    private ArrayList<String> fieldNames;


    public CSVParse(Optional<String> listDelimiter, char separator, char quotechar ) {
        this.parser = new CSVParser(separator,quotechar);
        this.fieldNames = null;
        this.listDelimiter = listDelimiter.isPresent()?(String)listDelimiter.get():"\u0001";
        this.listSplitter = Splitter.on(this.listDelimiter);
        this.valueFunction = new Function() {
            @Override
            public Object apply(Object input) {
                return ((String)input).contains(CSVParse.this.listDelimiter)?Lists.newArrayList(Iterables.transform(CSVParse.this.listSplitter.split((String)input), ParserUtils.nullEmptyStringFunction)):ParserUtils.nullEmptyStringFunction.apply((String) input);
            }
        };
    }

    public CSVParse(Optional<String> listDelimiter,char separator, char quotechar, Iterable<String> fieldNames) {
        this(listDelimiter,separator,quotechar);
        this.setFieldNames(fieldNames);
    }

    public CSVParse(Optional<String> listDelimiter,char separator, char quotechar, String header) {
        this(listDelimiter,separator,quotechar);
        this.setFieldNames(header);
    }

    public String getListDelimiter() {
        return this.listDelimiter;
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }

    public void setFieldNames(Iterable<String> fieldNames) {
        ParserUtils.validateFields(fieldNames);
        this.fieldNames = Lists.newArrayList(fieldNames);
    }

    public void setFieldNames(String header) {
        try {
            this.setFieldNames((Iterable) Arrays.asList(this.parser.parseLine(header)));
        } catch (Exception var3) {
            throw new ParseException(var3, "Unable to parse header [%s]", new Object[]{header});
        }
    }

    public Map<String, Object> parse(String input) {
        try {
            String[] e = this.parser.parseLine(input);
            if(this.fieldNames == null) {
                this.setFieldNames((Iterable)ParserUtils.generateFieldNames(e.length));
            }

            return Utils.zipMapPartial(this.fieldNames, Iterables.transform(Lists.newArrayList(e), this.valueFunction));
        } catch (Exception var3) {
            throw new ParseException(var3, "Unable to parse row [%s]", new Object[]{input});
        }
    }
}
