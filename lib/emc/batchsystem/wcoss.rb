#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

require 'emc/batchsystem/lsf'

class WCOSSBatchSys < BatchSys
  include LSFBatchSys
  def initialize(which_wcoss,remotecmd=nil)
    super(Array.new(which_wcoss).push('wcoss'),remotecmd)
  end
  def timezone()
    return 'UTC'
  end
  def defaultQueue()
    return 'hpc_ibm'
  end
  def moduleInitDir()
    return '/usrx/local/Modules/3.2.9/init'
  end
  def myBatchSysName()
    return where_am_i[0]
  end
  def jobCard(jc)
    cardbegin,cardafter=jobCardImpl(jc,16,32)
    return cardbegin+cardafter
  end
  def launchJobImpl(stream,justPrint=false,printStream=STDOUT)
    local_cmd='/usr/bin/env - /bin/bash --norc --login -c ". /usrx/local/Modules/3.2.9/init/bash ; module load lsf ; bsub"'
    remote_bsub="bsub"
    
    if(justPrint)
      bsub=printStream
      # STDERR.puts "#{remotecmd} #{cmd} <"
    else
      if(remotecmd.nil? || remotecmd=='' || remotecmd=~/\A\s*\z/)
        # Not submitting remotely, so use the "local submission" command:
        #warn "submitting via local command: #{local_cmd}"
        bsub=IO.popen(local_cmd,"w")
      else
        # Submit remotely using the simpler command
        #warn "submitting via remote command: #{remotecmd} #{remote_bsub}"
        bsub=IO.popen("#{remotecmd} #{remote_bsub}","w")
      end
    end

    jc=jobCard(makeJobStep(stream))
    stream.rewind()
    begin
      line=stream.readline()
    rescue EOFError
      bsub.close
      fail "Empty file given; no job to submit."
    end
    if(line=~/^#!/)
      bsub.write(line+jc+"\n")
      #warn "WRITE: #{line+jc}" unless justPrint
    else
      #warn "WRITE: #{jc}\n#{line.chomp}" unless justPrint
      bsub.write(jc+"\n"+line)
    end
    str=stream.read()
    #warn "WRITE: #{str}" unless justPrint
    bsub.write(str)
    #bsub.write(stream.read())

    bsub.close() unless justPrint
  end
  def requestRemoteSubmission(remoteBatchSys,remoteUser,localUser)
    case remoteBatchSys
    when 'tide','gyre','cirrus','stratus'
      return true
    else
      fail "#{myBatchSysName()} cannot remotely submit jobs to #{remoteBatchSys}"
    end
  end
  def requestLocalSubmission(user)
    return true
  end
end

