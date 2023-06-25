require 'emc/batchsystem/torque'

module ZeusQueuePartition
  def queuePartition(queue,partition)
    if(queue.nil?)
      if(partition.nil?)
        fail "You must specify either the queue or partition on Zeus."
      else
        return partition,nil
      end
    else
      if(partition.nil?)
        return queue,nil
      else
        if(partition==queue)
          return queue,nil
        else
          fail "On Zeus, you must specify the queue or partition, but not both.  You specified both, but they do not match (queue=#{queue} and partition=#{partition})."
        end
      end
    end
  end
end

class ZeusBatchSys < BatchSys
  include TorqueBatchSys
  include ZeusQueuePartition
  def initialize(which_zeus=[],remotecmd=nil)
    super(Array.new(which_zeus).push('zeus'),remotecmd)
  end
  def timezone()
    return 'UTC'
  end

  def defaultQueue
    return 'batch'
  end
  def defaultAccount
    return 'windfall'
  end

  def myBatchSysName()
    return 'zeus'
  end

  def jobCard(jc)
    cardbegin,cardafter = jobCardImpl(jc,12)
    return cardbegin+cardafter
  end
  def requestRemoteSubmission(remoteBatchSys,remoteUser,localUser)
    fail "Zeus cannot submit jobs remotely to other clusters."
  end
  def requestLocalSubmission(user)
    fail "On Zeus, you cannot submit jobs as another user."
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
end

