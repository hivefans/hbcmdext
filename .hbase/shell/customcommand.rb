require 'import'

module Shell
	module Commands
		class CustomCommand < Command
			@@ver = Float(VersionInfo.getVersion()[0..VersionInfo.getVersion().index('.', VersionInfo.getVersion().index('.')+1)-1])

			attr_reader :conf
			attr_reader :admin
			attr_reader :ver

			def initialize(shell)
				super(shell)
				@conf=HBaseConfiguration.new()
				@admin=HBaseAdmin.new(conf)
				@ofile=nil
				@formattype="detail".to_sym
				if(ENV["HBASE_FORMAT_TYPE"])
					@formattype=ENV["HBASE_FORMAT_TYPE"].to_sym
				end
				@formatsep=","
				if(ENV["HBASE_FORMAT_SEP"])
					@formatsep=ENV["HBASE_FORMAT_SEP"]
				end
			end

			def table(table)
				@tbname=table
				HTable.new(@conf, table)
			end

			def parentadmin
				shell.hbase_admin
			end

			def dispResult(rs, *coltype)
				puts @formattype
				case @formattype
				when :simple
					dispSimple(rs, *coltype)
				when :json
					dispJson(rs, *coltype)
				else
					dispDetail(rs, *coltype)
				end
			end

			def dispSimple(rs, *coltype)
				while(r=rs.next())
					if(@ofile)
						@ofile.printf Bytes.toString(r.getRow())
					elsif
						printf Bytes.toString(r.getRow())
					end
					fams = r.getMap()
					for fam in fams.keys()
						cols = fams[fam]
						for col in cols.keys()
							val = ""
							if(coltype && coltype[0])
								type = coltype[0]
								val = getColVal(Bytes.toString(fam), Bytes.toString(col), r.getValue(fam, col), type)
							elsif
							val = RubyEncoding.decodeUTF8(r.getValue(fam, col))
							end
							if(@ofile)
								@ofile.printf @formatsep+"%s" % [val]
							elsif
								printf @formatsep+"%s" % [val]
							end
						end
					end
					if(@ofile)
						@ofile.printf "\n"
					elsif printf "\n"
					end
				end
			end

			def dispJson(rs, *coltype)
				while(r=rs.next())
					if(@ofile)
						@ofile.printf "{\"key\":\"%s\"" % [Bytes.toString(r.getRow())]
					elsif
						printf "{\"key\":\"%s\"" % [Bytes.toString(r.getRow())]
					end
					fams = r.getMap()
					for fam in fams.keys()
						cols = fams[fam]
						for col in cols.keys()
							val = ""
							if(coltype && coltype[0])
								type = coltype[0]
								val = getColVal(Bytes.toString(fam), Bytes.toString(col), r.getValue(fam, col), type)
							elsif
							val = RubyEncoding.decodeUTF8(r.getValue(fam, col))
							end
							if(@ofile)
								@ofile.printf ", \"%s:%s\":\"%s\"" % [Bytes.toString(fam),Bytes.toString(col), val]
							elsif
								printf ", \"%s:%s\":\"%s\"" % [Bytes.toString(fam),Bytes.toString(col), val]
							end
						end
					end
					if(@ofile)
						@ofile.puts "}"
					elsif
						puts "}"
					end
				end
			end

			def dispDetail(rs, *coltype)
				printf("%sTable %s Results Begin%s\n", "="*30+" ", @tbname, "="*30)
				i = 0
				while(r=rs.next())
					printf("key[%s]:\n", Bytes.toString(r.getRow()))
					fams = r.getMap()
					for fam in fams.keys()
						printf("  family=[%s]:\n", Bytes.toString(fam))
						cols = fams[fam]
						for col in cols.keys()
							vals = cols[col]
							for ts in vals.keys()
								t = Time.at(ts/1000)
								val = ""
								if(coltype && coltype[0])
									type = coltype[0]
									val = getColVal(Bytes.toString(fam), Bytes.toString(col), vals[ts], type)
								elsif
								val = RubyEncoding.decodeUTF8(vals[ts])
								end
								printf("    col=[%s], timestamp=%d(%s), value=[%s]\n", Bytes.toString(col), ts, t.strftime("%Y%m%d:%H%M%S"), val)
							end#for ts
						end#for col
					end#for fam
					i += 1
				end
				printf("%sTable '%s' Results End, %d Records %s\n", "="*30+" ", @tbname, i, "="*30)
				return i
			end

			def getColVal(fam, col, val, coltype)
				type = coltype[fam+":"+col]
				if(type=="Long")
					return Long.toString(Bytes.toLong(val))
				elsif(type=="Int")
					return Integer.toString(Bytes.toInt(val))
				else
					return RubyEncoding.decodeUTF8(val)
				end
			end

			def getValBytes(val, type)
				if(type=="Long")
					return Bytes.toBytes(Long.parseLong(val.to_s))
				elsif(type=="Int")
					v = Bytes.toBytes(Integer.parseInt(val.to_s))
					if(v.length==8)
						return v[4,8]
					end
					return v
				else
					return val.to_s.to_java_bytes
				end
			end

			def getTableRegionInfo(htable)
				regioninfo = Hash.new()
				for hregion, svrname in htable.getRegionLocations()
					l = regioninfo[svrname]
					if(l==nil)
						l=Array.new
						regioninfo[svrname]=l
					end
					l.push(hregion)
				end
				return regioninfo
			end

			def getFuzzyFilter(fuzzykey)
				m = ""
				n = fuzzykey.size
				for i in(0..n-1)
					if(fuzzykey[i]=="?"[0])
						m += "\1"
					else
						m += "\0"
					end
				end
				rowkey = java.lang.String.new(fuzzykey)
				mask = java.lang.String.new(m)
				FuzzyRowFilter.new(Arrays.asList(Pair.new(rowkey.getBytes(), mask.getBytes())))
			end
		end
	end
end
