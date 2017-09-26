module Shell
	module Commands
		class TableStat < CustomCommand
			def help
				return <<-EOF
查询一个或多个表的状态:
语法: TableStat verbose, table1[, table2...]
参数:
	verbose	--详细信息开关 (true, false)
	table1	--可选的多个表名
hbase> TableStat false, 'ns:mytable'
hbase> TableStat true, 'ns:mytable1', 'ns:mytable2'
EOF
			end

			def command(verbose, *tables)
				status = admin.getClusterStatus()
				servers = status.getServers()
				totalsize = 0
				for table in tables
					puts("Table \"%s\"'s info:" % [table])
					tb = table(table)
					tabsize = 0
					regioncnt = 0
					regionmap = getTableRegionInfo(tb)
					for server, rl in regionmap
						stime = Time.at(server.getStartcode()/1000).strftime("%Y%m%d:%H%M%S")
						puts("%s [%s], %d regions" % [server.getHostAndPort(), stime, rl.length()])
						for name, region in status.getLoad(server).getRegionsLoad()
							parser = HRegionInfo.parseRegionName(region.getName())
							if(table==Bytes.toString(parser[0]))
								tabsize += region.getStorefileSizeMB()
								if(verbose)
									puts("    %s" % [ region.getNameAsString() ])
									puts("        %s" % [ region.toString() ])
								end
							end
						end
						regioncnt += rl.length()
					end #end regions
					puts("Table \"%s\" has %d regions, used %d MB spaces." % [table, regioncnt, tabsize])
					totalsize += tabsize
				end #end tables
				puts("All tables used %d MB spaces." % [totalsize])
			end
		end
	end
end
