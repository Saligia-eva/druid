# mobvista_druid

这是根据　[**druid.io**](http://druid.io/) 0.9.2 的基础上做的修改，　分析或者优化部分功能

## 对 csv 模式的修改

#### 原有 druid 系统的问题是:

数据解析过程中, 如果metric 字段为空, 则设置为0, 

#### 现在对 druid 关于csv的处理做了优化

- 可以自定义分隔符，在创建任务文档时需要声明分隔符,并没有默认分隔符.
- 可以自定义注释符号, 在创建任务文档时需要声明注释符号，并没有默认的注释符.
- 在注释符号中的分隔符被认为是逃逸字符,并不会当作分隔符处理.


关于任务文档的修改为:

```
"parseSpec":{
   "format" : "csv",
   "separator" : ",",
   "quotechar" : "\"",
   "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
    },
   "columns" : [],
   "dimensionsSpec" : {
      "dimensions" : []
   }
}
```


## 对时区的处理

druid 的时区在切换 `UTC+0800` 的时候我们发现会有错误产生, middleManager 在 MR 作业运行中,MR作业的时区还是收到hadoop环境的影响，并没有调到我们想要的时区环境中去运行。

我这里在探究过程中发现它的关于时间的控制还有分桶等的概念都是通过 `org.joda.time.DateTime` 的类来控制的。

在就该过程中我这里通过 `org.joda.time.DateTimeZone` 来控制时区， 通过将配置在文件中的时区配置传入到 MR 的 config 中去。在 Map 或者 Reduce 执行的时候便将时区注入进去。

同时通过 DateTimeZone, 我们将用户的输入的时区导入进去，用户可以自行配置输入数据的时间所依赖的时区。如果不进行配置，默认是与参数配置时区保持一致。

配置事例:

```
"parseSpec":{
   "format" : "csv",
   "separator" : ",",
   "quotechar" : "\"",
   "timestampSpec": {
        "column": "timestamp",
        "format": "auto",
        "timezone" : "UTC+0800"
    },
   "columns" : [],
   "dimensionsSpec" : {
      "dimensions" : []
   }
}
```


## 对空值的处理 

druid 在读取到空维度的时候默认是以空值处理，在这里我们使用字符串`NULL`来代替空值，同时用户可以根据自己的需要来将空值替换为用户自定义的维度值。

```
"parseSpec":{
   "format" : "csv",
   "separator" : ",",
   "quotechar" : "\"",
   "nullparse" : "null",
   "timestampSpec": {
        "column": "timestamp",
        "format": "auto"
    },
   "columns" : [],
   "dimensionsSpec" : {
      "dimensions" : []
   }
}
```
