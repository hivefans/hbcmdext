module Shell
	module Commands
		class RowCount < CustomCommand
			def help
				return <<-EOF
查询一个table的指定family的rowkey数, 使用此功能需要先将对查询的表指定系统coprocessor, 比如要查询'mytable':
alter 'mytable', METHOD=>'table_att', 'coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
对于大表, 此操作可能抛超时异常, 可设置客户端各种超时参数.
语法: RowCount table, family
参数:
	table	--表名
	family	--需要统计rowkey数的簇名
hbase> RowCount 'ns:mytable', 'family'
EOF
			end

			def command(table, family)
				client = AggregationClient.new(conf)
				scan = org.apache.hadoop.hbase.client.Scan.new
				name = Bytes.toBytes(table)
				fam = Bytes.toBytes(family)
				scan.addFamily(fam)
				cnt = 0
				if(@@ver>=0.96)
				  cnt = client.rowCount(table(table), LongColumnInterpreter.new, scan)
				else
				  cnt = client.rowCount(name, nil, scan)
				end
				puts("Table \"%s\"'s family \"%s\" has %d rows." % [table, family, cnt])
			end
		end
	end
end
