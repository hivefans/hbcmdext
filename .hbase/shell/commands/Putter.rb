module Shell
	module Commands
		class Putter < CustomCommand
			def help
				return <<-EOF
根据指定value类型插入col, 对系统put的补充 :
语法: Putter table, key, family, col, value[, type]
参数:
	table	--表名(可带namespace)
	rowkey	--rowkey字符串
	family	--簇字符串
	col		--列名字符串
	value	--值字符串
	type	--值类型, 目前支持[Long, Int], 缺省将value以string转成bytes
hbase> Putter 'ns:mytable', '01|18384|234|', 'info', 'colA', '19833938'
hbase> Putter 'mytable', '01|18384|234|', 'info', 'colB', 3938, 'Long'
EOF
			end

			def command(table, row, fam, col, value, *type)
				tb = table(table)
				p = org.apache.hadoop.hbase.client.Put.new(row.to_s.to_java_bytes)
				val = getValBytes(value, type[0])
				p.add(fam.to_s.to_java_bytes, col.to_s.to_java_bytes, val)
				tb.put(p)
			end
		end
	end
end
