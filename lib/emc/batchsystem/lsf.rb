#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

module LSFBatchSys
  def jobCardImpl(job,coresPerNode,packedPPN)
    cardbegin=''
    cardafter=''
    ppn=coresPerNode
    ppn=packedPPN if(!!job.cpuPacking)

    mpi=true  # job.isMPI()
    omp=job.isOpenMP()

    queue=job.queueOptions['queue']
    queue=defaultQueue() if queue.nil?
    cardbegin+="#BSUB -q #{queue}\n"
    res=job.queueOptions['reservation']
    if(!res.nil? && res!='')
      cardbegin+="#BSUB -U #{job.queueOptions['reservation']}\n"
    end

    cardbegin+="#BSUB -R affinity[core]\n"

    if(queue=='transfer')
      mpi=false
    end

    if(!job.parsed?)
      fail "You never called job.parse"
    else
      #warn "You did call job.parse"
    end
    #warn "Job: #{job}"
    #warn "Nodes: #{job.nodes.length}"
    #warn "Node 0: #{job.nodes[0]}"


      if(omp)
        # OpenMP job, so specify the threads
        threads=job.ompThreads
      else
        # Not an OpenMP job, so only one thread per process
        threads=1
      end

    # STEP 1: Generate the node/processor/thread configuration
    if(mpi)
      # MPI job.  Is it OpenMP too?
      
      maxAllow=(ppn.to_f/threads).floor # max MPI ranks per node

      maxppn=0 # maximum number of MPI ranks requested on any node by this job
      nodes=0  # number of nodes requested
      placement='' # processor placement string
      ip=0     # processor index
      job.nodes.each { |nodespec|
        nodeArray=nodespec.spreadNodes(ppn,threads)
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
      cardbegin+="#BSUB -a poe\n"
      cardbegin+="#BSUB -n #{nodes*maxppn}\n"
      cardbegin+="#BSUB -R span[ptile=#{maxppn}]\n"
      cardafter+=job.setEnvCommand("LSB_PJL_TASK_GEOMETRY","\"{#{placement}}\"")+"\n"
    elsif(omp)
      # Pure OpenMP job
      if(threads>32)
        fail "Cannot use more than 32 threads on WCOSS."
      end
      if(job.cpuPacking && threads>16)
        fail "Tried to use #{threads} threads.  You must use CPU packing (-P cpu=pack) to use more than 16."
      end
      cardbegin+="#BSUB -a openmp\n"
      cardbegin+="#BSUB -n #{threads}\n"
      cardbegin+="#BSUB -R span[ptile=#{threads}]\n"
    else
      # Serial job
      #cardbegin+="#BSUB -a poe\n"
      cardbegin+="#BSUB -n 1\n"
    end

    ############################################################
    # STEP 2: set stdout, stderr, other basic parameters
    over=''
    if(job.overwrite?)
      over='o'
    end
    if(job.stdout.nil?)
      if(job.stderr.nil?)
        # nothing to do
      else
        # stdout, stderr are the same and only stderr is specified
        cardbegin+="#BSUB -o#{over} #{convertPath(job.stderr)}\n"
        cardbegin+="#BSUB -e#{over} #{convertPath(job.stderr)}\n"
      end
    else
      if(job.stderr.nil?)
        # stdout, stderr are the same and only stdout is specified
        cardbegin+="#BSUB -o#{over} #{convertPath(job.stdout)}\n"
        cardbegin+="#BSUB -e#{over} #{convertPath(job.stdout)}\n"
      else
        # stdout, stderr both specified
        cardbegin+="#BSUB -o#{over} #{convertPath(job.stdout)}\n"
        cardbegin+="#BSUB -e#{over} #{convertPath(job.stderr)}\n"
      end
    end
    if(!job.stdin.nil?)
      cardbegin+="#BSUB -i #{convertPath(job.stdin)}\n"
    end
    if(!job.jobName.nil?)
      cardbegin+="#BSUB -J #{job.jobName}\n"
    end

    ############################################################
    ## SET RESOURCE LIMITS

    vmem=job.limitOptions['vmem']
    realmem=job.limitOptions['realmem']
    if(!vmem.nil?)
      cardbegin+="#BSUB -v #{vmem}\n"
    end
    if(!realmem.nil?)
      cardbegin+="#BSUB -M #{realmem}\n"
    end
    starttime=job.limitOptions['starttime']
    if(!starttime.nil?)
      tzstore=ENV['TZ']
      ENV['TZ']=timezone()
      stime=Time.at(starttime.to_i).strftime("%Y:%m:%d:%H:%M")
      cardbegin+=sprintf("#BSUB -b #{stime}\n")
      ENV['TZ']=tzstore
    end
    if(!job.limitOptions['walltime'].nil?)
      walltime=(job.limitOptions['walltime']/60.0).ceil
      wallhrs=(walltime/60).floor
      wallmins=walltime-wallhrs*60
      cardbegin+=sprintf("#BSUB -W %02d:%02d\n",wallhrs,wallmins)
    end
    if(!job.limitOptions['cputime'].nil?)
      cputime=(job.limitOptions['cputime']/60.0).ceil
      cpuhrs=(cputime/60).floor
      cpumins=cputime-cpuhrs*60
      cardbegin+=sprintf("#BSUB -c %02d:%02d\n",cpuhrs,cpumins)
    end

    ############################################################
    ## SET WORKING DIRECTORY

    cardbegin+="#BSUB -cwd #{convertPath(job.workDirOrPWD)}\n"

    ############################################################
    ## SET OTHER OPTIONS

    if(job.exclusive?)
      cardbegin+="#BSUB -x\n"
    end

    if(omp) then
      cardafter+=job.setEnvCommand("OMP_NUM_THREADS",threads)+"\n"
    end
    if(job.typeFlags['diskintensive'])
      cardafter+=job.setEnvCommand("MP_USE_BULK_XFER","yes")+"\n"
    end
    if(omp) then
      if(job.cpuPacking)
        cardafter+=job.setEnvCommand("MP_TASK_AFFINITY",'"cpu:'+threads.to_s+'"')+"\n"
      else
        cardafter+=job.setEnvCommand("MP_TASK_AFFINITY",'"core:'+threads.to_s+'"')+"\n"
      end
    else
      if(job.cpuPacking)
        cardafter+=job.setEnvCommand("MP_TASK_AFFINITY",'cpu')+"\n"
      else
        cardafter+=job.setEnvCommand("MP_TASK_AFFINITY",'core')+"\n"
      end
    end

    if(mpi)
      cardafter+=job.setEnvCommand("MP_EUIDEVICE","sn_all")+"\n"
      cardafter+=job.setEnvCommand("MP_EUILIB","us")+"\n"
    end

    moduledir=moduleInitDir()
    moduleload=''
    if(job.shell=~/\/bash/)
      moduleload+="if [[ -s #{moduledir}/bash ]] ; then\n  . #{moduledir}/bash\nfi\n"
    elsif(job.shell=~/\/tcsh/)
      moduleload+="if(-s #{moduledir}/tcsh ) then\n  source #{moduledir}/tcsh\nendif\n"
    elsif(job.shell=~/\/csh/)
      moduleload+="if(-s #{moduledir}/csh ) then\n  source #{moduledir}/csh\nendif\n"
    elsif(job.shell=~/\/sh/)
      moduleload+="if [[ -s #{moduledir}/sh ]] ; then\n  . #{moduledir}/sh\nfi\n"
    elsif(job.shell=~/\/ksh/)
      moduleload+="if [[ -s #{moduledir}/ksh ]] ; then\n  . #{moduledir}/ksh\nfi\n"
    else
      warn "Your job shell \"#{job.shell}\" does not match any known shells.  Assuming bash."
      moduleload+="if [[ -s #{moduledir}/bash ]] ; then\n  . #{moduledir}/bash\nfi\n"
    end

    moduleload+="module load ics\n"
    if(job.typeFlags['intelmpi']) then
      if(omp) then
        cardafter+=job.setEnvCommand("I_MPI_PIN_DOMAIN","auto")
      end
    else
      # Using IBM MPI
      moduleload+="module load ibmpe\n"
    end

    cardafter=moduleload+cardafter

    ############################################################

    return cardbegin,cardafter

    ############################################################
  end
end
