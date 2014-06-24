#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

require 'emc/batchsystem/torque'
require 'emc/batchsystem/gridengine'

class JetBatchSys < BatchSys
  # This is a virtual base class for the various Jets
  def initialize(which_jet,remotecmd=nil)
    super(Array.new(which_jet).push('jet'),remotecmd)
  end
  def timezone()
    return 'UTC'
  end
  def myBatchSysName()
    fail "Internal error: this Jet cluster class has not defined myBatchSysName"
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
      fail "You cannot submit a job as another user on the Jets."
    end
    return true
  end

  def requestLocalSubmission(user)
    # This routine's purpose is to call "fail" if the current user
    # cannot submit jobs as the specified user (who is guranteed to be
    # different).  If the user was unspecified, it will be nil.
    if(!user.nil?)
      fail "You cannot submit a job as another user on the Jets."
    end
    return true
  end
end

module JetQueuePartition
  def queuePartition(queue,partition)
    if(queue.nil?)
      if(partition.nil?)
        fail "You must specify either the queue or partition on Jet."
      else
        q,p = nil,partition
      end
    else
      if(partition.nil?)
        q,p = nil,queue
      else
        if(partition==queue)
          q,p = nil,queue
        else
          return queue,partition
        end
      end
    end

    # Try to guess the queue and partition based on the input
    m = /^rt(.*)/.match(p)
    if (m)
      return p,m[1] # nil,"rtujet" becomes "rtujet","ujet"
    elsif p=='service'
      return nil,'service'
    else
      return nil,p  # nil,"ujet" becomes nil,"ujet"
    end
  end
end

class SJetBatchSys < JetBatchSys
  include TorqueBatchSys
  include JetQueuePartition
  def initialize(which_sjet=[],remotecmd=nil)
    super(Array.new(which_sjet).push('sjet'),remotecmd)
  end

  def defaultQueue
    return 'batch'
  end
  def defaultAccount
    return nil
  end

  def myBatchSysName()
    return 'sjet'
  end
  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,16)
  #   cardbegin+="#PBS -l partition=sjet\n"
    return cardbegin+cardafter
  end
end

class UJetBatchSys < JetBatchSys
  include TorqueBatchSys
  include JetQueuePartition
  def initialize(which_sjet=[],remotecmd=nil)
    super(Array.new(which_sjet).push('ujet'),remotecmd)
  end

  def defaultQueue
    return 'batch'
  end
  def defaultAccount
    return nil
  end

  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,12)
    return cardbegin+cardafter
  end
  def myBatchSysName()
    return 'ujet'
  end
  def myBatchSysName()
    return 'ujet'
  end
end

class TJetBatchSys < JetBatchSys
  include TorqueBatchSys
  include JetQueuePartition
  def initialize(which_sjet=[],remotecmd=nil)
    super(Array.new(which_sjet).push('tjet'),remotecmd)
  end

  def defaultQueue
    return 'batch'
  end
  def defaultAccount
    return nil
  end

  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,12)
    return cardbegin+cardafter
  end
  def myBatchSysName()
    return 'tjet'
  end
  def myBatchSysName()
    return 'tjet'
  end
end

class NJetBatchSys < JetBatchSys
  include TorqueBatchSys
  include JetQueuePartition
  def initialize(which_sjet=[],remotecmd=nil)
    super(Array.new(which_sjet).push('njet'),remotecmd)
  end

  def defaultQueue
    return 'batch'
  end
  def defaultAccount
    return 'hwrfv3'
  end

  def myBatchSysName()
    return 'njet'
  end
  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,8)
    return cardbegin+cardafter
  end
end

