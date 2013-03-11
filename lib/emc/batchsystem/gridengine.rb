#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

module GridEngineBatchSys
  def convertPath(path)
    # Replace %J with $JOB_ID
    path.gsub!(/((?:^|[^%])(?:%%)*)(%J)/,'\1$JOB_ID')
    return path
  end
  def jobCardImpl(job,ppn)
    cardbegin=''
    cardafter=''

    if(job.isOpenMP())
      # OpenMP job, so specify the threads
      threads=job.ompThreads
    else
      # Not an OpenMP job, so only one thread per process
      threads=1
    end

    cardafter+=job.setEnvCommand("OMP_NUM_THREADS",threads.to_s)+"\n"
    cardafter+=job.setEnvCommand("MKL_NUM_THREADS",threads.to_s)+"\n"

    maxAllow=(ppn.to_f/threads).floor # max MPI ranks per node

    allprocs=[]
    nodes=0
    numprocs=0
    maxppn=0

    job.nodes.each { |nodespec|
      nodeArray=nodespec.spreadNodes(ppn,threads)
      fail "empty nodeArray" if nodeArray.empty?
      nodes+=nodeArray.length
      allprocs+=nodeArray
      nodeArray.each { |procs|
        if(procs<1)
          fail "internal error: somehow procs<1 (procs=#{procs})"
        end
        if(procs>maxAllow)
          fail "cannot place #{procs} ranks on a node (max is #{maxAllow} with #{threads} threads per rank)"
        end
        maxppn=procs if(procs>maxppn)
        numprocs+=procs
      }
    }

    queue=job.queueOptions['queue']
    queue=defaultQueue() if queue.nil?
    if(queue.nil?)
      fail "You must specify the queue (-Q queue=name) when using GridEngine."
    end

    if(nodes!=allprocs.length)
      fail "Internal error: nodes!=allprocs.length (#{nodes}!=#{allprocs.length})"
    end
    
    if(job.nodes.length==1 && job.nodes[0].singleSpec?)
      cardbegin+="#\$ -pe #{queue} #{job.nodes[0].singleSpec}\n"
    else
      cardbegin+="#\$ -pe #{queue} #{nodes*ppn}\n"
      cardafter+="tweak_hostfile -s #{allprocs.join(':')}\n"
    end

    ############################################################
    # STEP 2: set stdout, stderr, other basic parameters
    if(job.stdout.nil?)
      if(job.stderr.nil?)
        # nothing to do
      else
        # stdout, stderr are the same and only stderr is specified
        cardbegin+="#\$ -o #{convertPath(job.stderr)}\n"
        cardbegin+="#\$ -e #{convertPath(job.stderr)}\n"
      end
    else
      if(job.stderr.nil?)
        # stdout, stderr are the same and only stdout is specified
        cardbegin+="#\$ -o #{convertPath(job.stdout)}\n"
        cardbegin+="#\$ -e #{convertPath(job.stdout)}\n"
      else
        # stdout, stderr both specified
        cardbegin+="#\$ -o #{convertPath(job.stdout)}\n"
        cardbegin+="#\$ -e #{convertPath(job.stderr)}\n"
      end
    end
    if(!job.jobName.nil?)
      cardbegin+="#\$ -N #{job.jobName}\n"
    end

    ############################################################
    ## SET QUEING AND ACCOUNTING OPTIONS

    account=job.queueOptions['account']
    account=defaultAccount() if account.nil?
    if(!account.nil?)
      cardbegin+="#\$ -A #{account}\n"
    end

    res=job.queueOptions['reservation']
    if(!res.nil? && res!='')
      cardbegin+="#\$ -ac flags=ADVRES:#{res}\n"
    end

    ############################################################
    ## SET RESOURCE LIMITS

    vmem=job.limitOptions['vmem']
    realmem=job.limitOptions['realmem']
    if(vmem.nil? && !realmem.nil?)
      vmem=realmem
    end
    if(!vmem.nil?)
      cardbegin+="#\$ -l h_vmem=#{vmem}M\n"
    end
    if(!job.limitOptions['starttime'].nil?)
      warn "Warning: ignoring starttime because GridEngine does not support that."
    end
    if(!job.limitOptions['walltime'].nil?)
      walltime=(job.limitOptions['walltime']/60.0).ceil
      wallhrs=(walltime/60).floor
      wallmins=walltime-wallhrs*60
      cardbegin+=sprintf("#\$ -l h_rt=%02d:%02d:00\n",wallhrs,wallmins)
    end

    ############################################################
    ## SET WORKING DIRECTORY

    if(job.workDir.nil?)
      cardbegin+="#\$ -cwd\n"
    else
      cardafter+="cd #{convertPath(job.workDir)}\n"
    end

    return cardbegin,cardafter
  end
end
