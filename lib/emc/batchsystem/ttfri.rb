#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

require 'emc/batchsystem/torque'
require 'emc/batchsystem/gridengine'

class TTFRIBatchSys < BatchSys
  include TorqueBatchSys
  # This is a virtual base class for the various TTFRI clusters' BatchSys classes
  def initialize(which_ttfri,remotecmd=nil)
    super(Array.new(which_ttfri).push('ttfri'),remotecmd)
  end
  def timezone()
    return 'CST-8'
  end
  def allowProcs?()
    return false
  end
  def myBatchSysName()
    fail "Internal error: this TTFRI cluster class has not defined myBatchSysName"
  end
  def launchJobImpl(stream,justPrint=false,printStream=STDOUT)
    redome=true
    tries=0
    while(redome) do
      tries=tries+1
      if(justPrint)
        qsub=printStream
      else
        qsub=IO.popen("#{remotecmd} qsub","w")
      end
      
      step=makeJobStep(stream)
      jc=jobCard(step)
      jobSetup(step,justPrint)
      stream.rewind()
      line=stream.readline()
      if(line=~/^#!/)
        qsub.write(line+jc+"\n")
      else
        qsub.write(jc+"\n"+line)
      end
      qsub.write(stream.read())
      
      if justPrint
        redome=false
      else
        qsub.close()
        if(tries<2 && $? != 0) then
          redome=true
          warn "Submission failed (exit status #{$?} from ${remotecmd} qsub).  Sleeping 30 seconds and trying again."
          sleep(30)
        else
          redome=false
        end
      end
    end
  end

  def requestRemoteSubmission(remoteBatchSys,remoteUser,localUser)
    if(remoteUser.nil? || remoteUser!=localUser)
      return true
    else
      fail "You cannot submit a job as another user on the TTFRI clusters."
    end
    return true
  end

  def requestLocalSubmission(user)
    # This routine's purpose is to call "fail" if the current user
    # cannot submit jobs as the specified user (who is guranteed to be
    # different).  If the user was unspecified, it will be nil.
    if(!user.nil?)
      fail "You cannot submit a job as another user on the TTFRI clusters."
    end
    return true
  end
end

module TTFRIQueuePartition
  def queuePartition(queue,partition)
    if(queue.nil?)
      if(partition.nil?)
        return nil,nil
      else
        return nil,partition
      end
    else
      if(partition.nil?)
        return nil,queue
      else
        if(partition==queue)
          return nil,queue
        else
          return partition,partition
        end
      end
    end
  end
end

class TTFRISaolaBatchSys < TTFRIBatchSys
  include TTFRIQueuePartition
  def initialize(which_sttfri=[],remotecmd=nil)
    super(Array.new(which_sttfri).push('ttfrisaola'),remotecmd)
  end

  def defaultQueue
    return nil
  end
  def defaultAccount
    return nil
  end

  def myBatchSysName()
    return 'ttfrisaola'
  end
  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,16)
    return cardbegin+cardafter
  end
end

class TTFRIHPBatchSys < TTFRIBatchSys
  include TTFRIQueuePartition
  def initialize(which_sttfri=[],remotecmd=nil)
    super(Array.new(which_sttfri).push('ttfrihp'),remotecmd)
  end

  def defaultQueue
    return nil
  end
  def defaultAccount
    return nil
  end

  def myBatchSysName()
    return 'ttfrihp'
  end
  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,8)
    return cardbegin+cardafter
  end
end

