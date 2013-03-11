#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

require 'emc/batchsystem/loadleveler'

class CCSBatchSys < BatchSys
  include LoadLevelerBatchSys
  def initialize(which_ccs,remotecmd=nil)
    super(Array.new(which_ccs).push('ccs'),remotecmd)
  end
  def timezone()
    return 'EST5EDT'
  end
  def defaultQueue()
    return 'dev'
  end
  def defaultJobClass()
    return defaultQueue
  end
  def myBatchSysName()
    return where_am_i[0]
  end
  def jobCard(jc)
    cardbegin,cardafter=jobCardImpl(jc,32,64)
    return cardbegin+cardafter
  end
  def convertPath(path)
    # Replace %J with $(jobid)
    path.gsub!(/((?:^|[^%])(?:%%)*)(%J)/,'\1$(jobid)')
    return path
  end
  def launchJobImpl(stream,justPrint=false,printStream=STDOUT)
    if(justPrint)
      llsubmit=printStream
    else
      llsubmit=IO.popen("#{remotecmd} llsubmit -","w")
    end

    jc=jobCard(makeJobStep(stream))
    stream.rewind()
    line=stream.readline()
    if(line=~/^#!/)
      llsubmit.write(line+jc+"\n")
    else
      llsubmit.write(jc+"\n"+line)
    end
    llsubmit.write(stream.read())

    llsubmit.close() unless justPrint
  end
  def requestRemoteSubmission(remoteBatchSys,remoteUser,localUser)
    case remoteBatchSys
    when 'tide','gyre','cirrus','stratus'
      return true
    else
      fail  "#{myBatchSysName()} cannot remotely submit jobs to #{remoteBatchSys}"
    end
  end
end

