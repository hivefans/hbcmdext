module Shell
	module Commands
		class ScanPrefix < CustomCommand
			def help
				return <<-EOF
按key前缀查询一个表:
语法: ScanPrefix table, prefix[, opts]
参数:
	table	--表名
	prefix	--key前缀字符串
	opts	--可选参数map, 目前支持3个参数: pages-分页记录数;coltype-ap类型参数, 指定column的value类型, 目前支持[Long, Int], 缺省转换成utf8的string;out-输出文件名.
hbase> ScanPrefix 'ns:mytable', '01|18384|234|'
hbase> ScanPrefix 'ns:mytable', '01|18384|234|', {'pages'=>5}
hbase> ScanPrefix 'ns:mytable', '49|13318284499|', {'pages'=>3, 'coltype'=>{'info:init_value'=>'Long'}}
EOF
			end

			def command(table, prefix, *opts)
				args = opts[0]
				start = Bytes.toBytes(prefix)
				tb=table(table)
				fl = FilterList.new(FilterList::Operator::MUST_PASS_ALL)
				fl.addFilter(PrefixFilter.new(start))
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
				scan = org.apache.hadoop.hbase.client.Scan.new(start, fl)
				scan.setCaching(200)
				rs = tb.getScanner(scan)
				dispResult(rs, coltype)
			end
		end
	end
end
