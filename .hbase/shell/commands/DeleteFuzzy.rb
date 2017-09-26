module Shell
	module Commands
		class DeleteFuzzy < CustomCommand
			def help
				return <<-EOF
按模糊key删除一个表的记录, 对于key中不确定的字符可用'?'表示:
语法: DeleteFuzzy table, fuzzykey
参数:
	table	--表名(可带namespace）
	fuzzykey--一个模糊模式的key字符串
hbase> DeleteFuzzy 'ns:mytable', '??|str1|str2|str3'
EOF
			end

			def command(table, fuzzykey)
				tb=table(table)
				scan = org.apache.hadoop.hbase.client.Scan.new
				scan.setFilter(getFuzzyFilter(fuzzykey))
				scan.setCaching(200)
				rs = tb.getScanner(scan)
				deletes = ArrayList.new
				count = 0
				while(r=rs.next())
					deletes.add(org.apache.hadoop.hbase.client.Delete.new(r.getRow()))
					count += 1
					if(count%1000==0)
						tb.delete(deletes)
						deletes.clear()
						puts Time.now.to_s+" deleted %d rows." % [count]
					end
				end
				tb.delete(deletes)
				puts Time.now.to_s+" total deleted %d rows" % [count]
			end
		end
	end
end
