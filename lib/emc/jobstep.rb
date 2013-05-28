#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

class JobStepNodes
  attr_accessor :nodes,:procs
  def initialize(nodes,procs)
    @nodes=nodes
    @procs=procs
  end
  def totalProcs
    return @nodes*@procs
  end
  def spreadNodes(maxThreads,threadsPerProc)
    fail 'nil maxThreads' if maxThreads.nil?
    fail 'nil threadsPerProc' if threadsPerProc.nil?
    wanted=procs*threadsPerProc
    if(wanted>maxThreads)
      fail "Requested #{procs} processors with #{threadsPerProc} threads each, which goes beyond the maximum thread count of #{maxThreads} per node (#{procs}*#{threadsPerProc}=#{wanted}>#{maxThreads})"
    end
    #warn "NODES p=#{procs} * n=#{nodes} = #{[@procs]*@nodes}\n"
    return [@procs] * @nodes
  end
  def singleSpec?
    return @nodes==1
  end
  def singleSpec
    if(@nodes!=1)
      fail "Cannot specify @nodes:@procs in a single specification"
    end
    return @procs
  end
end
class JobStepNodeSpread
  attr_accessor :procs
  def initialize(procs,groups)
fail "nil procs" if procs.nil?
    @groups=groups
    @procs=procs
  end
  def totalProcs
    if(@groups.nil?)
      return @procs
    else
      return @groups*@procs
    end
  end
  def singleSpec
    return @procs
  end
  def singleSpec?
    return @groups==1 || @groups.nil?
  end
  def spreadNodes(maxThreads,threadsPerProc)
    # Use the "minimum job card complication" method when spreading
    # nodes to work around a bug in Torque:
    return spreadNodesMinComplication(maxThreads,threadsPerProc)
  end
  def spreadNodesMinComplication(maxThreads,threadsPerProc)
    # This function will give you a nodes spread like:
    #   11,11,11,12,12,12
    # if you request 69 processors on a machine with 12 per node.
    # That simplifies the job card in Torque.  The older routine,
    # spreadNodesEvenly, will give you another distribution

    fail 'nil maxThreads' if maxThreads.nil?
    fail 'nil threadsPerProc' if threadsPerProc.nil?

    maxProcsPerNode=(maxThreads.to_f/(threadsPerProc)).floor
    if(!@groups.nil? && maxProcsPerNode<@procs)
      fail "Can only fit #{maxProcsPerNode} ranks per node, but you requested #{@procs}"
    end

    # groups = number of processor groups to distribute
    # ppg = number of MPI ranks per group
    # threads = number of processors (threads) per MPI rank
    # ppn = maximum number of processors allowed per compute node
    # nodes = number of compute nodes needed to satisfy the request

    threads=threadsPerProc
    ppn=maxThreads
    groups=@groups
    ppg=@procs
    if(groups.nil?)
      groups=ppg
      ppg=1
    end

    gpn=(ppn/ppg/threads).floor
    #warn "maximum of #{gpn} groups per node"

    nodes=(groups.to_f/gpn).ceil
    #warn "need #{nodes} nodes"

    avgper=groups.to_f/nodes
    #warn "need #{avgper} groups of #{ppg} processors per node"
    
    # remain = how many processors we would NOT have allocated if we
    #    request avgper.floor per node:
    remain=groups-nodes*avgper.floor
    #warn "if we requested #{avgper.floor} on each of #{nodes} nodes, we would lack #{remain} groups of #{ppg} processors"
    
    if(remain==0)
      # We can allocate the minimum number
      total=nodes*avgper.floor
      #warn "nodes=#{nodes}:ppn=#{avgper.floor*ppg}"
      return [avgper.floor*ppg] * nodes
    else
      total=(nodes-remain)*avgper.floor + remain*avgper.ceil
      #warn "nodes=#{nodes-remain}:ppn=#{avgper.floor*ppg}+#{remain}:ppn=#{avgper.ceil*ppg}"
      return [avgper.floor*ppg] * (nodes-remain) + 
        [avgper.ceil*ppg] * remain
    end
    
    if(total*ppg!=ppg*groups)
      fail "Internal error: total*ppg=#{total*ppg} != ppg*groups=#{ppg*groups}"
    end
  end

  def spreadNodesEvenly(maxThreads,threadsPerProc)
    # This function will give you a node spread like:
    #   11,12,11,12,11,12
    # if you need 69 processors on a machine with 12 per node.
    # Torque doesn't like that because the -l nodes=(stuff) request
    # is too long.  I'm using the above spreadNodesMinComplication
    # instead due to the simpler output.

    fail 'nil maxThreads' if maxThreads.nil?
    fail 'nil threadsPerProc' if threadsPerProc.nil?
    maxProcsPerNode=(maxThreads.to_f/(threadsPerProc)).floor
    if(!@groups.nil? && maxProcsPerNode<@procs)
      fail "Can only fit #{maxProcsPerNode} ranks per node, but you requested #{@procs}"
    end
    #warn "mt=#{maxThreads} tpp=#{threadsPerProc}"
    if(@groups.nil?)
      nodeCount=(@procs.to_f/maxProcsPerNode).ceil
    else
      #warn "maxppn=#{maxProcsPerNode.to_f} maxppn/procs=#{(maxProcsPerNode.to_f/@procs)} floor=#{(maxProcsPerNode.to_f/@procs).floor} groups/ceil=#{(@groups.to_f/(maxProcsPerNode.to_f/@procs).ceil)} .ceil="
      nodeCount=(@groups.to_f/(maxProcsPerNode.to_f/@procs).floor).ceil
    end
    out=Array.new(nodeCount,0)

    if(@groups.nil?)
      procsLeft=@procs
    else
      procsLeft=@procs*@groups
    end
    #warn "ENTER LOOP procsLeft=#{procsLeft} nodeCount=#{nodeCount} maxppn=#{maxProcsPerNode} @procs=#{@procs} @groups=#{@groups}\n"
    for n in 0..nodeCount-1
      #warn "LOOP n=#{n} procsLeft=#{procsLeft} nodeCount=#{nodeCount} maxppn=#{maxProcsPerNode} @procs=#{procs} @groups=#{@groups}\n"
      nodesLeft=nodeCount-n
      if(@groups.nil?)
        procs=(procsLeft.to_f/nodesLeft).round.to_i
      else
        procs=(((procsLeft.to_f/nodesLeft)/@procs).floor*@procs).to_i
      end
      procsLeft-=procs
      out[n]=procs
      #warn "GOT procs=#{procs} nodesLeft=#{nodesLeft} procsLeft=#{procsLeft}"
      if(procs>maxProcsPerNode)
        fail "Internal error: ended up with too many processors on node #{n} (#{procs}>#{maxProcsPerNode})"
      end
    end
    #warn "SPREAD p=#{procs} g=#{@groups} ppn=#{threadsPerProc} mt=#{maxThreads} = #{out}\n"
    return out
    #warn "ALSO GOT HERE"
  end
end


class JobStep # a cluster-independent representation of a job step
  @@maxParserLines=100000
  @@today=nil
  @@tomorrow=nil

  attr_accessor :queue, :shell, :where_am_i, :ompThreads, :cpuPacking, :workDir
  attr_reader   :typeOptions, :typeFlags, :queueOptions, :queueFlags
  attr_reader   :limitOptions, :limitFlags
  attr_accessor :stdout, :stderr, :stdin, :remoteUser, :remoteHost, :jobName, :remoteBatchSys

  def nodes
    if(@nodes==nil)
      fail "nodes is nil"
    elsif(@nodes.length<1)
      fail "nodes is empty"
    end
    return @nodes
  end

  def overwrite?
    return !!typeFlags['overwrite']
  end

  def parsed?
    return @parsed
  end

  def initialize(where_am_i)
    @parsed=false
    @where_am_i=where_am_i

    # basic job information:
    @shell=nil
    @jobName=nil
    @exclusive=nil
    @workDir=nil

    # Job logging and stdin:
    @stdin=nil ; @stdout=nil ; @stderr=nil

    # Remote job submission:
    @remoteUser=nil
    @remoteHost=nil

    # Resource limit information:
    @limitFlags={}
    @limitOptions={}

    # Job type information:
    @typeFlags={}
    @typeOptions={}

    # Job queuing information:
    @ompThreads=nil
    @cpuPacking=nil
    @queueFlags={}
    @queueOptions={}
    @nodes=[]
  end

  def totalProcs()
    n=0
    nodes.each() { |node| n+=node.totalProcs() }
    return n
  end

  def setEnvCommand(var,value)
    if(!shell.nil? && shell=~/csh/)
      return "setenv #{var} #{value}"
    else
      return "export #{var}=#{value}"
    end
  end

  def workDirOrPWD()
    wd=workDir()
    if(wd.nil?)
      return Dir.pwd()
    else
      return wd
    end
  end

  def isMPI()
    return !!self.typeFlags['mpi']
  end

  def isOpenMP()
    return !!self.typeFlags['openmp']
  end

  def shStyleShell()
    return !! shell=~/\/(sh|ksh|bash)/
  end
  def cshStyleShell()
    return !! shell=~/\/(csh|tcsh)/
  end

  def exclusive=(bool)
    @exclusive=!!bool
  end
  def exclusiveSpecified?()
    return !@exclusive.nil?
  end
  def exclusive?()
#    if(@exclusive.nil?)
#      return self.isMPI() || self.isOpenMP()
#    else
    return @exclusive
#    end
  end

  def addNodeArrayEntry(addme)
    if(@nodes.length>0 || addme.totalProcs>1)
      self.typeFlags['mpi']=true
    end
    @nodes.push(addme)
  end    

  def addNodes(nodeCount,procCount)
    addNodeArrayEntry(JobStepNodes.new(nodeCount,procCount))
  end
  def addNodeSpread(procCount,groupCount)
    addNodeArrayEntry(JobStepNodeSpread.new(procCount,groupCount))
  end

  def eachLine(file)
    # This is the preparser that processes the file, finding the #EMC$
    # lines and discarding @machine: lines that do not match the
    # current host

    firstEMCLine=true
    lineno=1
    begin
      fname=file.path
    rescue
      fname='STDIN'
    end
    file.each { |line|
      line.chomp!
      #warn "preprocess #{line}"
      if(lineno==1)
        if(line=~/^#!\s*([^ ]+)(?:\s+.*).*$/)
          @shell=$1
          lineno+=1
          #warn "shell line"
          next
        else
          #warn "#{fname}:#{lineno}: warning: first line is not a #! line: #{line}"
          #warn "#{fname}:#{lineno}: don't know what shell is in use, assuming /bin/sh"
          @shell='/bin/sh'
        end
      elsif(lineno>@@maxParserLines)
        #warn "#{fname}: too long, stopped parsing at #{maxParserLines}"
        #warn "#{fname}: will continue with partial information"
        break
      end

      # skip lines unless they begin with #EMC$
      match=line=~/^#EMC\$\s*(.*?)\s*$/
      if( ! match )
        #warn "Not #EMC$ line"
        next
      else
        rest=$1
        #warn "remainder: #{rest}"
      end
      rest=$1

      if(rest=~/^\s*$/) # skip blank lines
        #warn "ignore blank line"
        next
      end
      if(rest=~/^\s*#.*$/) # skip comment lines
        #warn "ignore comment line"
        next
      end

      # In lines like #EMC$ @jet:
      # Skip the line unless we're on that machine
      if(rest=~/^\s*@\s*([a-z!A-Z0-9_, \t-]+)\s*:(.*)$/)
        machines,rest = $1,$2
        machines=machines.split(/\s*,\s*/)
        #warn "machines=#{machines} rest=#{rest}"
        right_place=false
        stop_searching=false
        machines.each { |machine|
          machine=machine.downcase
          if(machine=~/^!(.*)/)
            machine=$1
            right_place=true
            where_am_i.each { |place|
              if(place==machine)
                #warn "@#{machine}: #{place}==#{machine} (notted)"
                right_place=false
                stop_searching=true
                break
              else
                #warn "@#{machine}: #{place}!=#{machine} (notted)"
              end
            }
          else
            where_am_i.each { |place|
              if(place==machine)
                #warn "@#{machine}: #{place}==#{machine}"
                right_place=true
              else
                #warn "@#{machine}: #{place}!=#{machine}"
              end
            }
          end
          if(stop_searching)
            break
          end
        }
        if(!right_place)
          lineno+=1
          #warn "#{fname}:#{lineno}: ignoring #{machines} line: #{line.chomp}"
          next
        end
      end

      #warn "Yield rest=#{rest}"
      yield line,rest,lineno,firstEMCLine
      #warn "Done yielding."

      firstEMCLine=false
      lineno+=1
    }
  end

  def findRemoteHost(file)
    begin
      fname=file.path
    rescue
      fname='STDIN'
    end
    firstEMCLine=true
    user,host = nil,nil
    eachLine(file) { |line,rest,lineno,firstEMCLine|
      if(rest=~/^\s*-R\s*([a-zA-Z0-9_.-]+)\s*(?:#.*)?$/)
        user,host = nil,$1.downcase
        break
      elsif(rest=~/^\s*-R\s*([a-zA-Z0-9_.-]+)@([a-zA-Z0-9_.-]+)\s*(?:#.*)?$/)
        user,host = $1,$2.downcase
        break
      elsif(rest=~/^\s*-R.*$/)
        warn "#{fname}:#{lineno}: ignoring unrecognized -R option: #{rest}\n"
      end
    }
    file.rewind()
    #warn "got here"
    #warn "Setting where_am_i to #{host}"
    where_am_i=[host]

    @remoteUser,@remoteHost=user,host

    return user,host
  end

  def parse(file)
    begin
      fname=file.path
    rescue
      fname='STDIN'
    end
    #warn "PARSING #{fname}"
    eachLine(file) { |line,rest,lineno,firstEMCLine|
      #warn "parse: rest #{rest} lineno #{lineno} first #{firstEMCLine}"
      # This line is an EMC line or a syntax error

      if(rest=~/^\s*-O\s+(\S+|"(?:[^\"\\]+|\\\\|\\\")*")\s*(?:#.*)?$/)
        self.stdout=$1
      elsif(rest=~/^\s*-E\s+(\S+|"(?:[^\"\\]+|\\\\|\\\")*")\s*(?:#.*)?$/)
        self.stderr=$1
      elsif(rest=~/^\s*-I\s+(\S+|"(?:[^\"\\]+|\\\\|\\\")*")\s*(?:#.*)?$/)
        self.stdin=$1
      elsif(rest=~/^\s*-N\s+(\S+|"(?:[^\"\\]+|\\\\|\\\")*")\s*(?:#.*)?$/)
        self.jobName=$1
      elsif(rest=~/^\s*-D\s+(\S+|"(?:[^\"\\]+|\\\\|\\\")*")\s*(?:#.*)?$/)
        self.workDir=$1
      elsif(rest=~/^\s*-T(.*)/)
        typestr=$1.downcase
        begin
          typestr.parseOptions() { |key,value|
            case key
            when 'exclusive','notshared','not_shared'
              self.exclusive=true
            when 'shared','nonexclusive','notexclusive','not_exclusive'
              self.exclusive=false
            when 'diskintensive','disk','bulkxfer','bulk_xfer'
              self.typeFlags['diskintensive']=true
            else
              if(value.nil?)
                self.typeFlags[key]=true
              else
                self.typeOptions[key]=value
              end
            end
          }
        rescue
          warn "#{fname}:#{lineno}: warning: ignoring unrecognized -T (job type) option \"#{opt}\""
        end
      elsif(rest=~/^\s*-Q(.*)/)
        queuestr=$1
#        begin
          queuestr.parseOptions() { |key,value|
            if(value.nil?)
              self.queueFlags[key.downcase]=true
            else
              self.queueOptions[key.downcase]=value
            end
          }
#        rescue
#          warn "#{fname}:#{lineno}: warning: ignoring unrecognized -Q (queuing info) option \"#{rest}\""
#        end
      elsif(rest=~/^\s*-L(.*)/)
        limitstr=$1
        begin
          limitstr.parseOptions() { |key,value|
            if(value.nil?)
              self.limitFlags[key.downcase]=true
            else
              type,dvalue=key.downcase,value.downcase
              case type
              when 'walltime','wallclock','wallclocktime'
                self.limitOptions['walltime']=dvalue.to_timespan
              when 'cputime','cpulimit'
                self.limitOptions['cputime']=dvalue.to_timespan
              when 'start','starttime'
                self.limitOptions['starttime']=dvalue.to_epoch_time
              when 'mem','memory','realmem','realmemory'
                self.limitOptions['realmem']=dvalue.to_megabytes
              when 'vmem','vmemory','virtualmem','virtualmemory'
                self.limitOptions['vmem']=dvalue.to_megabytes
              else
                self.limitOptions[type]=dvalue
              end
            end
          }
#        rescue
#          warn "#{fname}:#{lineno}: warning: ignoring unrecognized -L (resource limit) option \"#{limitstr}\""
        end
      elsif(rest=~/^\s*-P(.*)/)
        procstr=$1.downcase
        #warn "Proc string: #{rest}"
        begin
          procstr.parseOptions() { |key,value|
            case key
            when 'threads'
              self.ompThreads=value.to_i
              if(self.ompThreads.nil? || self.ompThreads<2)
                self.typeFlags['openmp']=false
              else
                self.typeFlags['openmp']=true
              end
            when 'affinity','cpu'
              if(value=='cpu' || value=='pack')
                #warn "Enabling cpu packing due to affinity=cpu or cpu=pack option"
                self.cpuPacking=true
              elsif(value=='core')
                #warn "Disabling cpu packing due to affinity=core option"
                self.cpuPacking=false
              else
                warn "#{fname}:#{lineno}: ignoring unknown task affinity option \"#{value}\""
              end
            when 'node','nodes'
              if(value=~/^\s*(\d+)\s*$/)
                self.addNodes(1,$1.to_i)
              elsif(value=~/^\s*(\d+)\s*[xX]\s*(\d+)\s*$/)
                self.addNodes($1.to_i,$2.to_i)
              else
                warn "#{fname}:#{lineno}: warning: Ignoring unrecognized -P node(s)= value \"#{value}\""
              end
            when 'spread'
              if(value=~/^\s*(\d+)\s*$/)
                self.addNodeSpread($1.to_i,nil)
              elsif(value=~/\s*(\d+)\s*x\s*(\d+)\s*$/)
                self.addNodeSpread($2.to_i,$1.to_i)
              else
                warn "#{fname}:#{lineno}: warning: Ignoring unrecognized -P spread= value \"#{value}\""
              end
            else
              warn "#{fname}:#{lineno}: warning: Ignoring unrecognized -P (processor config) option \"#{key}=#{value}\""
            end
          }
#        rescue
#          warn "#{fname}:#{lineno}: warning: Ignoring unrecognized -P (processor config) option \"#{procstr}\""
        end
      elsif(rest=~/^\s*-R\s*([a-zA-Z0-9_.-]+)\s*(?:#.*)?$/)
        # Ignore this line; we've already found the remote host in findRemoteHost
        if(!firstEMCLine)
          fail "#{fname}:#{lineno}: You must specify the -R (remote host) option as the FIRST #EMC$ line."
        end
      elsif(rest=~/^\s*-R\s*([a-zA-Z0-9_.-]+)@([a-zA-Z0-9_.-]+)\s*(?:#.*)?$/)
        # Ignore this line; we've already found the remote host in findRemoteHost
        if(!firstEMCLine)
          fail "#{fname}:#{lineno}: You must specify the -R (remote host) option as the FIRST #EMC$ line."
        end
      else
        warn "#{fname}:#{lineno}: warning: ignoring unrecognized input: #{rest}"
      end
    }
#warn "EXITED PARSER"
    @parsed=true
    return self
  end
end

