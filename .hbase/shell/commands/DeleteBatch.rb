module Shell
	module Commands
		class DeleteBatch < CustomCommand
			def help
				return <<-EOF
将输入文件中的每行作为rowkey批量提交删除:
语法: DeleteBatch table, keysfile
参数:
	table		--表名(可带namespace)
	keysfile	--存储rowkey的文件名(一行一个rowkey)
hbase> DeleteBatch 'ns:mytable', 'keys.txt'
EOF
			end

			def command(table, keysfile)
				tb=table(table)
				deletes = ArrayList.new
				count = 0
				File.open(keysfile, "r") do |file|
					while line=file.gets
						deletes.add(org.apache.hadoop.hbase.client.Delete.new(line.chomp.to_s.to_java_bytes))
						count += 1
						if(count%1000==0)
							tb.delete(deletes)
							deletes.clear()
							puts Time.now.to_s+" deleted %d rows." % [count]
						end
					end
				end
				tb.delete(deletes)
				puts Time.now.to_s+" total deleted %d rows." % [count]
			end
		end
	end
end

