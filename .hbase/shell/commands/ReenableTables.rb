module Shell
	module Commands
		class ReenableTables < CustomCommand
			def help
				return <<-EOF
重置一个或多个表(依次执行disable, enable操作):
语法: ReenableTables table1[, table2...]
参数:
	table1	--可选的多个表名
hbase> ReenableTables 'ns:mytable'
hbase> ReenableTables 'mytable', 'ns:mytable'
EOF
			end

			def command(*tables)
				list=Array.new
				if(tables.length==0)
					for t in admin.listTables
						list.push(t.getNameAsString)
					end
				else
					list=tables
				end
				
				for t in list
						puts Time.now.to_s+" disable table: "+t
						admin.disableTable(t) unless admin.isTableDisabled(t)
						puts Time.now.to_s+" enable table: "+t
						admin.enableTable(t)
				end
			end
		end
	end
end
