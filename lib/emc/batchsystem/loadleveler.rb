#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

module LoadLevelerBatchSys
  def jobCardImpl(job,coresPerNode,cpusPerNode)
    cardbegin=''
    cardafter=''
    ppn=coresPerNode
    ppn=cpusPerNode if(!!job.cpuPacking)

    # STEP 1: Generate the node/processor/thread configuration
    if(job.isOpenMP())
      # OpenMP job, so specify the threads
      threads=job.ompThreads
    else
      # Not an OpenMP job, so only one thread per process
      threads=1
    end
      
    maxAllow=(ppn.to_f/threads).floor # max MPI ranks per node

    total_tasks=0
    maxppn=0 # maximum number of MPI ranks requested on any node by this job
    nodes=0  # number of nodes requested
    placement='' # processor placement string
    ip=0     # processor index
    job.nodes.each { |nodespec|
      nodeArray=nodespec.spreadNodes(ppn,threads)
      total_tasks+=nodespec.totalRanks()
      fail "empty nodeArray" if nodeArray.empty?
      nodes+=nodeArray.length
      nodeArray.each { |procs|
        if(procs<1)
          fail "internal error: somehow procs<1 (procs=#{procs})"
        end
        if(procs>maxAllow)
          fail "cannot place #{procs} ranks on a node (max is #{maxAllow} with #{threads} threads per rank)"
        end
        maxppn=procs if(procs>maxppn)
        placement+="("+Array(ip..ip+procs-1).join(',')+")"
        ip+=procs
      }
    }
    #cardbegin+="#BSUB -a poe\n"

    if(ip==1 && !job.isMPI)
      cardbegin+="#\@ job_type = serial\n"
    elsif(job.nodes[0].singleSpec?)
      cardbegin+="#\@ job_type = parallel\n"
      cardbegin+="#\@ total_tasks = #{ip}\n"
      if(nodes>1)
        cardbegin+="#\@ node = #{nodes}\n"
      end
    else
      cardbegin+="#\@ job_type = parallel\n"
      cardbegin+="#\@ task_geometry = {#{placement}}\n"
      cardbegin+="#\@ node = #{nodes}\n"
    end

    cardbegin+="#\@ parallel_threads = #{threads}\n"

    ############################################################
    # STEP 2: set stdout, stderr, other basic parameters
    if(job.stdout.nil?)
      if(job.stderr.nil?)
        # nothing to do
      else
        # stdout, stderr are the same and only stderr is specified
        cardbegin+="#\@ output = #{convertPath(job.stderr)}\n"
        cardbegin+="#\@ error = #{convertPath(job.stderr)}\n"
      end
    else
      if(job.stderr.nil?)
        # stdout, stderr are the same and only stdout is specified
        cardbegin+="#\@ output = #{convertPath(job.stdout)}\n"
        cardbegin+="#\@ error = #{convertPath(job.stdout)}\n"
      else
        # stdout, stderr both specified
        cardbegin+="#\@ output = #{convertPath(job.stdout)}\n"
        cardbegin+="#\@ error = #{convertPath(job.stderr)}\n"
      end
    end
    if(!job.stdin.nil?)
      cardbegin+="#\@ input = #{convertPath(job.stdin)}\n"
    end
    if(!job.jobName.nil?)
      cardbegin+="#\@ job_name = #{job.jobName}\n"
    end

    ############################################################
    ## SET QUEING AND ACCOUNTING OPTIONS

    jclass=job.queueOptions['class']
    jclass=job.queueOptions['queue'] if jclass.nil?
    jclass=defaultJobClass() if jclass.nil?
    cardbegin+="#\@ class = #{jclass}\n"

    jgroup=job.queueOptions['group']
    jgroup=defaultJobGroup() if jgroup.nil?
    cardbegin+="#\@ group = #{jgroup}\n"

    jaccount=job.queueOptions['account']
    jaccount=defaultJobAccount() if jaccount.nil?
    cardbegin+="#\@ account_no = #{jaccount}\n"

    ############################################################
    ## SET RESOURCE LIMITS

    vmem=job.limitOptions['vmem']
    realmem=job.limitOptions['realmem']
    realmem=vmem if(realmem.nil?)
    if(!realmem.nil?)
      cardbegin+="#\@ node_resources = ConsumableMemory(#{(realmem*1024).ceil} KB)\n"
    end
    starttime=job.limitOptions['starttime']
    if(!starttime.nil?)
      tzstore=ENV['TZ']
      ENV['TZ']=timezone()
      stime=Time.at(starttime.to_i).strftime("%m/%d/%Y %H:%M:00")
      cardbegin+=sprintf("#\@ startdate = #{stime}\n")
      ENV['TZ']=tzstore
    end
    if(!job.limitOptions['walltime'].nil?)
      walltime=(job.limitOptions['walltime']/60.0).ceil
      wallhrs=(walltime/60).floor
      wallmins=walltime-wallhrs*60
      cardbegin+=sprintf("#\@ wall_clock_limit = %02d:%02d:00\n",wallhrs,wallmins)
    end
    if(job.typeFlags['total_tasks'])
      cardafter+=job.setEnvCommand("TOTAL_TASKS",total_tasks.to_s)+"\n"
    end

    ############################################################
    ## SET WORKING DIRECTORY

    if(!job.workDir.nil?)
      dir=convertPath(job.workDir)
      cardbegin+="#\@ initialdir = #{dir}\n"
      cardafter+="cd #{dir}\n"
    end

    ############################################################
    ## SET OTHER OPTIONS

    if(job.typeFlags['diskintensive'])
      cardbegin+="#\@ bulkxfer = yes\n"
    end
    if(job.cpuPacking)
      cardbegin+="#\@ task_affinity = cpu(#{threads})\n"
    else
      cardbegin+="#\@ task_affinity = core(#{threads})\n"
    end
    if(job.exclusive?)
      cardbegin+="#\@ network.MPI=sn_all,not_shared,us\n"
    end

    ############################################################

    cardbegin+="#\@ queue\n"
    return cardbegin,cardafter

    ############################################################
  end
end

