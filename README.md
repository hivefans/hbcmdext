# 一个hbase shell的扩展命令集

***

### 简述
按 *hbase shell* 规范扩展了 *hbase shell* 的命令集, 新增了命令组 *custom*.

查看所有扩展命令的帮助信息:
```
hbase(main):001:0> help 'custom'
```
查看 *ScanPrefix* 等具体命令帮助信息:
```
hbase(main):001:0> help 'ScanPrefix'
```

***

### 部署
直接将文件 **.irbrc** 和目录 **.hbase** 放到 *$HOME* 下即可.

***

### 环境设置
依赖2个环境变量:

1. *HBASE_FORMAT_TYPE* (查询类命令的输出格式)  
    支持3个值:  
    **simple** - 普通的多字段列表输出  
    **json** - json格式  
    **detail** - **缺省值**, 记录的每个字段单独显示一行, 并带时间戳  

2. *HBASE_FORMAT_SEP* (简单格式的字段分隔符, **缺省值: ","** )


