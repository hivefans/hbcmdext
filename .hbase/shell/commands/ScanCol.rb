module Shell
	module Commands
		class ScanCol < CustomCommand
			def help
				return <<-EOF
按col值查询一个表:
语法: ScanCol table, family, column, value[, pages, coltype]
参数:
	table	--表名
	family	--表的簇名
	column	--簇内的列名
	value	--列对应的值或值的前缀
	pages	--分页记录数, 可选项, 无此参数则返回全部结果
	coltype	--map类型参数, 指定column的value类型, 目前支持[Long, Int], 缺省转换成utf8的string
hbase> ScanCol 'ns:mytable', 'info', 'colA', '19834400'
hbase> ScanCol 'ns:mytable', 'info', 'colB', '289823', 5
hbase> ScanCol 'ns:mytable', 'info', 'colC', 8983, 3, {'info:init_value'=>'Long'}
EOF
			end

			def command(table, fam, col, val, *pages)
				tb = table(table)
				comparator = nil
				coltype = nil
				if(pages[1])
				    coltype = pages[1]
					if(coltype[fam+":"+col]!=nil)
						comparator = BinaryComparator.new(getValBytes(val, coltype[fam+":"+col]))
					else
						comparator = SubstringComparator.new(val)
					end
				else
					comparator = SubstringComparator.new(val)
				end
				fl = FilterList.new(FilterList::Operator::MUST_PASS_ALL)
				fl.addFilter(SingleColumnValueFilter.new(Bytes.toBytes(fam), Bytes.toBytes(col), CompareFilter::CompareOp.valueOf('EQUAL'), comparator))
				if(pages.length>0)
				    fl.addFilter(PageFilter.new(pages[0]))
				end
				scan = org.apache.hadoop.hbase.client.Scan.new
				scan.setFilter(fl)
				scan.setCaching(500)
				rs = tb.getScanner(scan)
				dispResult(rs, coltype)
			end
		end
	end
end
