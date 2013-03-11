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

    qsub.close() unless justPrint
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
        return nil,partition
      end
    else
      if(partition.nil?)
        return nil,queue
      else
        if(partition==queue)
          return nil,queue
        else
          fail "On Jet, you must specify the queue or partition, but not both.  You specified both, but they do not match (queue=#{queue} and partition=#{partition})."
        end
      end
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
    return 'nhfip'
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

