module Shell
	module Commands
		class CreateTable < CustomCommand
			def help
				return <<-EOF
创建表:
语法: CreateTable name, regions[, args]
参数:
	name	--表名(可带namespace)
	regions	--region个数, region的rowkey前缀以类似'00|', '01|'做前缀
	args	--需要设置的表属性
hbase> CreateTable 'mytable', 50
hbase> CreateTable 'ns:mytable1', 50, {NAME=>'info', IN_MEMORY => 'true', COMPRESSION=>'LZO', VERSIONS=>1}
EOF
			end

			def command(name, numregions, *args)
				# Fail if table name is not a string
				raise(ArgumentError, "Table name must be of type String") unless name.kind_of?(String)
				
				# Flatten params array
				args = args.flatten.compact
				
				# Fail if no column families defined
				raise(ArgumentError, "Table must have at least one column family") if args.empty?

				htd = org.apache.hadoop.hbase.HTableDescriptor.new(name)
				splits = Java::byte[][numregions-1].new
				idx = 0
				wide = Math.log10(numregions).ceil
				fmtstr = sprintf("%%0%dd|", wide)
				while idx<numregions-1
					prefix = sprintf(fmtstr, idx+1)
					splits[idx] = prefix.to_java_bytes
					idx = idx+1
				end

				args.each do |arg|
					unless arg.kind_of?(String) || arg.kind_of?(Hash)
						raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash or String type")
					end
					
					# Add column to the table
					descriptor = parentadmin.hcd(arg, htd)
					if arg[COMPRESSION_COMPACT]
					  descriptor.setValue(COMPRESSION_COMPACT, arg[COMPRESSION_COMPACT])
					end
					htd.addFamily(descriptor)
				end

				@admin.createTable(htd, splits)
			end
		end
	end
end
