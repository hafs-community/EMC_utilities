require 'emc/qmonitor/queuestate.rb'
require 'emc/nonil.rb'
require 'open3'

module EMC
  module Queues
    class SlurmQueueState < QueueState
      include EMC::NoNil

      STATE_MAP={
        'BOOT_FAIL' => 'RM',
        'CANCELED' => 'RM',
        'COMPLETED' => 'C',
        'CONFIGURING' => 'CF',
        'COMPLETING' => 'R',
        'DEADLINE' => 'C',
        'FAILED' => 'RM',
        'NODE_FAIL' => 'RM',
        'OUT_OF_MEMORY' => 'RM',
        'PENDING' => 'Q',
        'PREEMPTED' => 'RM',
        'RUNNING' => 'R',
        'RESV_DEL_HOLD' => 'H',
        'REQUEUE_FED' => 'Q',
        'REQUEUE_HOLD' => 'Q',
        'REQUEUED' => 'Q',
        'RESIZING' => 'R',
        'REVOKED' => 'RM',
        'SIGNALING' => 'R',
        'SPECIAL_EXIT' => 'RM',
        'STAGE_OUT' => 'R',
        'STOPPED' => 'R',
        'SUSPENDED' => 'Q',
        'TIMEOUT' => 'RM'
      }

      FIELDS=[
              [ 40, 'jobid' ],
              [ 40, 'username' ],
              [ 10, 'numcpus' ],
              [ 20, 'partition' ],
              [ 30, 'submittime' ],
              [ 30, 'starttime' ],
              [ 30, 'endtime' ],
              [ 30, 'priority' ],
              [ 10, 'exit_code' ],
              [ 30, 'state' ],
              [ 200, 'name' ],
              [ 200, 'stdin' ],
              [ 200, 'stdout' ],
              [ 200, 'stderr' ],
              [ 200, 'workdir' ],
              [ 40, 'qos' ],
              [ 40, 'groupname' ],
              [ 40, 'account' ],
              [ 60, 'reservation' ],
             ]

      def initialize(options,user)
        super
      end

      def call_squeue()
        squeue_command="squeue -h -a -t all -M all"
        
        if not @user.nil?
          squeue_command += " -u '#{@user}'"
        end

        squeue_command += " -O '"

        squeue_command += FIELDS.collect { |a,b| "#{b}:#{a}" }.join(',')
        squeue_command += "'"
        return `#{squeue_command}`
      end

      def call_queue_manager()
        jobs=Hash.new # Hash of hashes: jobid => {details for job with that id}

        job=nil  # Hash for current job
        pack_id=nil  # id of pack's id (or job id if it is not a pack element)
        job_id=nil  # id of current job
        update_job=false # Are we updating an existing job (pack element)

        if @opts.only_cache
          return jobs
        end

        jobs={}
        call_squeue.each_line do |line|
          n1=0
          job={}
          FIELDS.each do |width,field|
            n2=n1+width-1
            part=line[n1..n2]
            part='' if part.nil?
            part=part.strip
            if part!= '(null)'
              job["slurm/#{field}"]=part
            end
            n1=n2+1
          end
          jobid=job.fetch('slurm/jobid',' ')
          username=job.fetch('slurm/username',' ')
          if jobid =~ /\S/ and username=~/\S/
            job['jobid']=jobid
            jobs[jobid]=job
          end
        end
        process_slurm_info jobs

        return jobs
      end # call_queue_manager

      def process_slurm_info(jobs)
        jobs.each do |jobid,job|
          if job.nil?
            raise "Nil job for job id #{pack_id}"
          end
          job['long_state']=job.fetch('slurm/state','??')
          if STATE_MAP.include? job['long_state']
            job['state']=STATE_MAP[job['long_state']]
          else
            job['state']=job['long_state']
          end
          job['queue']=job.fetch('slurm/qos','??')
          job['class']=job['queue']
          job['exeguess']='??'
          job['project']=job.fetch('slurm/account','??')
          job['reservation']=job.fetch('slurm/reservation','')
          job['name']=job.fetch('slurm/name','??')
          job['out']=job.fetch('slurm/stdout','??')
          job['err']=job.fetch('slurm/stderr','??')
          job['in']=job.fetch('slurm/stdin','??')
          job['workdir']=job.fetch('slurm/workdir','??')
          job['group']=job.fetch('slurm/groupname','??')
          if job.include? 'slurm/submittime'
            job['qtime']=fromdate(job['slurm/submittime'])
          else
            job['qtime']='??'
          end

          if job.include? 'slurm/username'
            user_name_id=job['slurm/username']
            if user_name_id =~ /([^\(]+)\((\S+)\)/
              job['user']=$1
              job['uid']=$2
            else
              job['user']=user_name_id
            end
          end
          job['procs']=job.fetch('slurm/numcpus','??')
          job['procs_from']='slurm/numcpus'
        end
        return jobs
      end

      def fromdate(date)
        begin
          return ((DateTime.parse(date)-DateTime.parse("1970-01-01T00:00:00+00:00"))*24*3600).to_i.to_s
          # Simpler code for ruby 1.9: 
          # return DateTime.parse(date).to_time.to_i.to_s
        rescue
          return nil
        end
      end # fromdate

    end # class SlurmQueueState
  end # module Queues
end # module EMC
