module Shell
	module Commands
		class ScanFuzzy < CustomCommand
			def help
				return <<-EOF
按模糊key查询一个表, 对于key中不确定的字符可用'?'表示:
语法: ScanFuzzy table, fuzzykey[, opts]
参数:
	table	--表名
	fuzzykey--一个模糊模式的key字符串
	opts	--可选参数map, 目前支持3个参数: pages-分页记录数;coltype-ap类型参数, 指定column的value类型, 目前支持[Long, Int], 缺省转换成utf8的string;out-输出文件名.
hbase> ScanFuzzy 'ns:mytable', '??|020|133111111|'
hbase> ScanFuzzy 'ns:mytable', '??|????|13312345678|', {'pages'=>5}
hbase> ScanFuzzy 'ns:mytable', '??|????|13312345678|', {'pages'=>3, 'coltype'=>{'info:init_value'=>'Long'}}
EOF
			end

			def command(table, fuzzykey, *opts)
				args = opts[0]
				tb=table(table)
				fl = FilterList.new(FilterList::Operator::MUST_PASS_ALL)
				fl.addFilter(getFuzzyFilter(fuzzykey))
				coltype = nil
				if(args)
					if(args['pages'])
						fl.addFilter(PageFilter.new(args['pages']))
					end
					if(args['out'])
						@ofile=File.new(args['out'], "w")
					end
					if(args['coltype'])
					    coltype = args['coltype']
					end
				end
				scan = org.apache.hadoop.hbase.client.Scan.new
				scan.setFilter(fl)
				scan.setCaching(200)
				rs = tb.getScanner(scan)
				dispResult(rs, coltype)
			end
		end
	end
end
