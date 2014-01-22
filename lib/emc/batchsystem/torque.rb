#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

module TorqueBatchSys
  def queuePartition(queue,partition)
    # This function takes in the requested queue and partition and
    # puts out the actual queue and partition to be used in the job
    # card (in that order). This is needed due to some weirdnesses
    # about how Zeus and the Jets handle queues and partitions in
    # Torque.
    return queue,partition
  end
  def allowProcs?()
    return true
  end
  def convertPath(path)
    # Replace %J with $PBS_O_JOBID
    path.gsub!(/((?:^|[^%])(?:%%)*)(%J)/,'\1$PBS_O_JOBID')
    return path
  end

  def jobSetup(job,justPrint)
    if(job.overwrite?)
      if(job.stdout.nil?)
        if(job.stderr.nil?)
          # no output specified, nothing to do
        else
          deleteOutErr(job.stderr,nil,job.workDir,justPrint)
        end
      else
        if(job.stderr.nil?)
          deleteOutErr(job.stdout,nil,job.workDir,justPrint)
        else
          deleteOutErr(job.stderr,job.stdout,job.workDir,justPrint)
        end
      end
    end
  end
  def deleteOutErr(path1,path2,workdir,justPrint)
    cwork=nil
    cwork=convertPath(workdir) unless workdir.nil?

    for path in [path1,path2]
      next if path.nil?
      convert=convertPath(path)

      deleteme=nil
      if(convert[0,1]!='~' && convert[0,1]!='/')
        # path is relative
        if(!cwork.nil?)
          deleteme=cwork+'/'+convert
        else
          deleteme=convert # file is relative to CWD
        end
      else
        # path is absolute
        deleteme=convert
      end

      if(!deleteme.nil?)
        if(!justPrint && File.exist?(deleteme))
          warn "#{deleteme}: deleting old file due to -T overwrite"
          begin
            File.delete(deleteme)
          rescue
            # ignore errors while deleting old log files
          end
        end
      else
        warn "nil deleteme"
      end
    end
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
    cardafter+=job.setEnvCommand("MKL_NUM_THREADS","1")+"\n"

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

    if(nodes!=allprocs.length)
      fail "Internal error: nodes!=allprocs.length (#{nodes}!=#{allprocs.length})"
    end

    if(allowProcs?() && job.nodes.length==1 && job.nodes[0].singleSpec? && threads==1)
      procs=job.nodes[0].singleSpec
      if(procs==1 && job.exclusive?)
        # to be exclusive, we must have two ranks or more
        cardbegin+="#PBS -l nodes=1:ppn=2\n"
      else
        cardbegin+="#PBS -l procs=#{procs}\n"
      end
    elsif(!allowProcs?() && job.nodes.length==1 && job.nodes[0].singleSpec? && threads==1 && job.nodes[0].singleSpec()==1)
      cardbegin+="#PBS -l nodes=1:ppn=#{maxAllow}\n"
    else
      cardbegin+="#PBS -l nodes=";
      prev=0  # number of ppn in previous node
      accum=0 # number of times that occurred in sequence
      first=true
      assigned=0 # number of processors actually requested
      for i in 0..nodes-1
        if(allprocs[i]==prev)
          accum+=1
        elsif(prev>0 && accum>0)
          if(first)
            first=false
          else
            cardbegin+="+"
          end
          cardbegin+="#{accum}:ppn=#{prev}"
          assigned+=accum*prev
          prev=allprocs[i]
          accum=1
        else
          prev=allprocs[i]
          accum=1
        end
      end
      if(prev>0 && accum>0)
        cardbegin += (first ? "" : "+") + "#{accum}:ppn=#{prev}"
        assigned+=accum*prev
      end
      if(assigned!=numprocs)
        fail "internal error: wanted #{numprocs} but requested #{assigned}"
      end
      cardbegin+="\n"
    end

    if job.exclusive?
      cardbegin+="#PBS -n\n"
    end

    ############################################################
    # STEP 2: set stdout, stderr, other basic parameters
    if(job.stdout.nil?)
      if(job.stderr.nil?)
        # nothing to do
      else
        # stdout, stderr are the same and only stderr is specified
        cardbegin+="#PBS -o #{convertPath(job.stderr)}\n"
        cardbegin+="#PBS -e #{convertPath(job.stderr)}\n"
        cardbegin+="#PBS -joe\n"
      end
    else
      if(job.stderr.nil?)
        # stdout, stderr are the same and only stdout is specified
        cardbegin+="#PBS -o #{convertPath(job.stdout)}\n"
        cardbegin+="#PBS -e #{convertPath(job.stdout)}\n"
        cardbegin+="#PBS -joe\n"
      else
        # stdout, stderr both specified
        cardbegin+="#PBS -o #{convertPath(job.stdout)}\n"
        cardbegin+="#PBS -e #{convertPath(job.stderr)}\n"
        if(job.stdout==job.stderr)
          cardbegin+="#PBS -joe\n"
        end
      end
    end
    if(!!job.typeFlags['nameworkaround'])
      if(!job.stdout.nil?)
        cardbegin+="#PBS -N #{convertPath(job.stdout)}\n"
      elsif(!job.stderr.nil?)
        cardbegin+="#PBS -N #{convertPath(job.stderr)}\n"
      end
    elsif(!job.jobName.nil?)
      cardbegin+="#PBS -N #{job.jobName}\n"
    end


    ############################################################
    ## SET QUEING AND ACCOUNTING OPTIONS

    queue,partition=queuePartition(job.queueOptions['queue'],job.queueOptions['partition'])

    if(!queue.nil? && queue!='')
      cardbegin+="#PBS -q #{queue}\n"
    end
    if(!partition.nil? && partition!='')
      cardbegin+="#PBS -l partition=#{partition}\n"
    end

    account=job.queueOptions['account']
    account=defaultAccount() if account.nil?
    if(!account.nil? && account!='')
      cardbegin+="#PBS -A #{account}\n"
    end

    res=job.queueOptions['reservation']
    if(!res.nil? && ! ( res=~/^\s*$/ ))
      cardbegin+="#PBS -l flags=ADVRES:#{res}\n"
    end

    ############################################################
    ## SET RESOURCE LIMITS

    vmem=job.limitOptions['vmem']
    realmem=job.limitOptions['realmem']
    if(vmem.nil? && !realmem.nil?)
      vmem=realmem
    end
    if(!vmem.nil?)
      cardbegin+="#PBS -l vmem=#{vmem}M\n"
    end
    starttime=job.limitOptions['starttime']
    if(!starttime.nil?)
      tzstore=ENV['TZ']
      ENV['TZ']=timezone()
      stime=Time.at(starttime.to_i).strftime("%Y%m%d%H%M.%S")
      cardbegin+=sprintf("#PBS -a #{stime}\n")
      ENV['TZ']=tzstore
    end
    if(!job.limitOptions['walltime'].nil?)
      walltime=(job.limitOptions['walltime']/60.0).ceil
      wallhrs=(walltime/60).floor
      wallmins=walltime-wallhrs*60
      cardbegin+=sprintf("#PBS -l walltime=%02d:%02d:00\n",wallhrs,wallmins)
    end

    ############################################################
    ## SET WORKING DIRECTORY

    if(job.workDir.nil?)
      cardbegin+="#PBS -d .\n"
      cardafter+="cd #{Dir.pwd}\n"
    else
      cardbegin+="#PBS -d #{convertPath(job.workDir)}\n"
      cardafter+="cd #{convertPath(job.workDir)}\n"
    end

    ############################################################
    ## SET OTHER OPTIONS

    cardbegin+="#PBS -m n\n"

    return cardbegin,cardafter
  end
end

