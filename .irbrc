$LOAD_PATH.unshift File.expand_path(File.join(File.dirname(__FILE__), '.hbase'))
require 'set'
require "java"
require 'irb/ext/save-history'
require 'shell/customcommand'

cmdpath=File.dirname(__FILE__)+"/.hbase/shell/commands"
cmds = Set.new
Dir.foreach(cmdpath) do |file|
	cmd = file[0,file.index('.')]
	next if(cmd.empty?)
	cmds.add(cmd)
end

Shell.load_command_group('custom', :full_name=>'自定义命令', :commands=>cmds)

Shell::Shell.new(Hbase::Hbase.new, Shell::Formatter).export_commands(self)
