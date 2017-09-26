module Shell
	module Commands
		class RowLike < CustomCommand
			def help
				return <<-EOF
按key的正则表达式查询一个表(可能会很慢):
语法: RowLike table, regex[, pages, coltype]
参数:
	table	--表名
	regex	--key的正则表达式字符串
	pages	--分页记录数, 可选项, 无此参数则返回全部结果
	coltype	--map类型参数, 指定column的value类型, 目前支持[Long, Int], 缺省转换成utf8的string
hbase> RowLike 'ns:mytable', '[0-9]{2}\|[0-9]{3,4}\|138293|.*'
hbase> RowLike 'ns:mytable', '[0-9]{2}\|[0-9]{3,4}\|138293|.*', 5
hbase> RowLike 'ns:mytable', '[0-9]{2}\|[0-9]{3,4}\|1983.*', 3, {'info:init_value'=>'Long'}
EOF
			end

			def command(table, regex, *pages)
				tb=table(table)
				fl = FilterList.new(FilterList::Operator::MUST_PASS_ALL)
				fl.addFilter(RowFilter.new(CompareFilter::CompareOp.valueOf('EQUAL'), RegexStringComparator.new(regex)))
				if(pages.length>0)
				    fl.addFilter(PageFilter.new(pages[0]))
				end
				scan = org.apache.hadoop.hbase.client.Scan.new
				scan.setFilter(fl)
				scan.setCaching(500)
				rs = tb.getScanner(scan)
				coltype = nil
				if(pages[1]!=nil)
				    coltype = pages[1]
				end
				dispResult(rs, coltype)
			end
		end
	end
end
