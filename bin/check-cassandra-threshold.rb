#!/usr/bin/env ruby
#
# check-cassandra-threshold.rb
#
# ===
#
# DESCRIPTION:
#   Run nodetool command (e.g. 'cfstats'), parse the result for a particular value,
#   and test that value against minumum and/or maximum thresholds
#
# PLATFORMS:
#   Linux, BSD, Solaris
#
# DEPENDENCIES:
#   gem: sensu-plugin
#
# USAGE:
#  check-cassandra-threshold.rb [--host <host>] [--port <port>] --compactionstats --tpstats --cfstats --filter_regex <filter_regex> \
#  [--minwarn <minimun-warning-threshold>] [--maxwarn <maximum-warning-threshold>] \
#  [--mincrit <minimun-critical-threshold>] [--maxcrit <maximum-critical-threshold>] 
 
#
# EXAMPLE:
#  check-cassandra-threshold.rb 
#

require 'sensu-plugin/check/cli'
require 'socket'

class CassandraThreshold < Sensu::Plugin::Check::CLI

  option :hostname,
         short: '-h HOSTNAME',
         long: '--host HOSTNAME',
         description: 'cassandra hostname',
         default: 'localhost'

  option :port,
         short: '-P PORT',
         long: '--port PORT',
         description: 'cassandra JMX port',
         default: '7199'

  option :compactionstats,
         description: 'compactionstats" metrics (default: yes)',
         on: :tail,
         short: '-o',
         long: '--[no-]compactionstats',
         boolean: true,
         default: false

  option :tpstats,
         description: 'Cassandra threadPool metrics (default: yes)',
         on: :tail,
         short: '-t',
         long: '--[no-]tpstats',
         boolean: true,
         default: false

  option :cfstats,
         description: 'metrics on keyspaces and column families (default: no)',
         on: :tail,
         short: '-c',
         long: '--[no-]cfstats',
         boolean: true,
         default: false

  option :print_metrics,
         description: 'output all metrics (default: no)',
         on: :tail,
         long: '--print_metrics',
         boolean: true,
         default: false

  option :filter_regex,
         description: 'Metric pattern for threshold check',
         long: '--filter_regex FILTER_REGEX'

  option :minwarn,
         long: '--minwarn MINWARN',
         proc: proc(&:to_f)

  option :maxwarn,
         long: '--maxwarn MAXWARN',
         proc: proc(&:to_f)

  option :mincrit,
         long: '--mincrit MINCRIT',
         proc: proc(&:to_f)

  option :maxcrit,
         long: '--maxcrit MAXCRIT',
         proc: proc(&:to_f)


  # execute cassandra's nodetool and return output as string
  def nodetool_cmd(cmd)
    `nodetool -h #{config[:hostname]} -p #{config[:port]} #{cmd}`
  end


  def check_threshold(metric, value)
    if config[:mincrit] != nil
      critical "#{metric} #{value} less than #{config[:mincrit]}" if value.to_f < config[:mincrit]
    end
    if config[:maxcrit] != nil
      critical "#{metric} #{value} exceeds #{config[:maxcrit]}" if value.to_f > config[:maxcrit]
    end
    if config[:minwarn] != nil
      warning "#{metric} #{value} less than #{config[:minwarn]}" if value.to_f < config[:minwarn]
    end
    if config[:maxwarn] != nil
      warning "#{metric} #{value} exceeds #{config[:maxwarn]}" if value.to_f > config[:maxwarn]
    end
    output metric if config[:print_metrics]
  end


  def get_metric(string) # rubocop:disable NestedMethodDefinition
    string.strip!
    (metric, value) = string.split(': ')
    if metric.nil? || value.nil? # rubocop:disable Style/GuardClause
      return [nil, nil]
    else
      # sanitize metric names for graphite
      metric.gsub!(/[^a-zA-Z0-9]/, '_')  # convert all other chars to _
      metric.gsub!(/[_]*$/, '')          # remove any _'s at end of the string
      metric.gsub!(/[_]{2,}/, '_')       # convert sequence of multiple _'s to single _
      metric.downcase!
      # sanitize metric values for graphite. Numbers only, please.
      value = value.chomp(' ms.').gsub(/([0-9.]+)$/, '\1')
    end
    [metric, value]
  end


  def parse_compactionstats
    cstats = nodetool_cmd('compactionstats')
    cstats.each_line do |line|
      if m = line.match(/^(.+):\s+([0-9]+)/)
        (metric, value) = get_metric(line)
        if config[:filter_regex]
          next unless metric =~ /#{config[:filter_regex]}/
        end
        check_threshold(metric, value) if !metric.nil?
      end
    end
  end


  def parse_tpstats
    tpstats = nodetool_cmd('tpstats')
    tpstats.each_line do |line|
      next if line =~ /^Pool Name/
      next if line =~ /^Message type/

      if m = line.match(/^(\w+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$/)
        (thread, active, pending, completed, blocked) = m.captures

        metric = "threadpool.#{thread}.active"
        value = active
        if config[:filter_regex]
          check_threshold(metric, value) if metric =~ /#{config[:filter_regex]}/
        else
          check_threshold(metric, value)
        end
        metric = "threadpool.#{thread}.pending"
        value = pending
        if config[:filter_regex] 
          check_threshold(metric, value) if metric =~ /#{config[:filter_regex]}/
        else
          check_threshold(metric, value)
        end
        metric = "threadpool.#{thread}.completed"
        value = completed
        if config[:filter_regex] 
          check_threshold(metric, value) if metric =~ /#{config[:filter_regex]}/
        else
          check_threshold(metric, value)
        end
        metric = "threadpool.#{thread}.blocked"
        value = blocked
        if config[:filter_regex] 
          check_threshold(metric, value) if metric =~ /#{config[:filter_regex]}/
        else
          check_threshold(metric, value)
        end
      end

      if m = line.match(/^(\w+)\s+(\d+)$/)
        (message_type, dropped) = m.captures
        metric = "message_type.#{message_type}.dropped"
        value = dropped
        if config[:filter_regex]
          check_threshold(metric, value) if metric =~ /#{config[:filter_regex]}/
        else
          check_threshold(metric, value)
        end
      end
    end
  end


  def parse_cfstats

    cfstats = nodetool_cmd('cfstats')

    keyspace = nil
    cf = nil
    foo = ""
    outside_threshold = false 

    cfstats.each_line do |line|
      metric_full = nil
      num_indents = line.count("\t")
      if m = line.match(/^Keyspace:\s+(\w+)$/)
        keyspace = m[1]
      elsif m = line.match(/\t\tColumn Family[^:]*:\s+(\w+)$/)
        cf = m[1]
      elsif m = line.match(/\t\tTable[^:]*:\s+(\w+)$/)
        cf = m[1]
      elsif num_indents == 0
        # keyspace = nil
        cf = nil
      elsif num_indents == 2 && !cf.nil?
        # a column family metric
        (metric, value) = get_metric(line)
        metric_full = "#{keyspace}.#{cf}.#{metric}"
      elsif num_indents == 1 && !keyspace.nil?
        # a keyspace metric
        (metric, value) = get_metric(line)
        metric_full = "#{keyspace}.#{metric}"
      end
      if config[:filter_regex]
        next unless metric_full =~ /#{config[:filter_regex]}/
      end
      check_threshold(metric_full, value) if !metric_full.nil?
    end
  end


  def run
    parse_compactionstats if config[:compactionstats]
    parse_cfstats if config[:cfstats]
    parse_tpstats if config[:tpstats]
    ok
  end
end
