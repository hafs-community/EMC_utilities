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
        'CONFIGURING' => 'R',
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

      def initialize(options,user)
        super
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

        stdin,stdout,stderr=Open3.popen3("scontrol --all show job")
        stdin.close
        text=stdout.read
        stderr.read

        jobs = text_to_jobs text
        process_slurm_info jobs

        if not @user.nil?
          jobs.delete_if do |jobid,job|
            ( not job.include? 'user' ) or
              job['user'].nil? or job['user'].downcase!=@user.downcase
          end
        end

        return jobs
      end # call_queue_manager

      def text_to_jobs(text)
        job=nil
        pack_id=nil
        job_id=nil
        pack_offset=nil
        rest=nil
        update_job=nil

        text.each_line do |line|
          if line =~ /^No jobs/i
            return Hash.new
          end

          if line =~ /^JobId=(\S+)(.*)/
            # Start of new job or part of a multi-part job
            pack_id=$1
            job_id=$1
            pack_offset=nil
            rest=$2
            update_job=false
            job=nil

            if rest =~ /PackJobId=(\S+)/
              pack_id=$1
              job_id=$1
              update_job=true
              job=jobs[job_id]
            end  

            if job.nil?
              job=Hash.new
              jobs[job_id]=job
              job['jobid']=job_id
            end

            if rest =~ /PackJobId=(\S+)/
              job['slurm/pack_id']=pack_id
            end
            if rest =~ /PackJobOffset=(\S+)/
              pack_offset=$1
              if not job.include? 'slurm/pack_offset_list'
                job['slurm/pack_offset_list'] = [ pack_offset ]
              else
                job['slurm/pack_offset_list'] << pack_offset
              end
            end
          else
            rest=line
          end # if line starts with JobID

          if job.nil?
            raise "Nil job for line #{rest.inspect}"
          end

          splat=rest.split(" ")
          collected=splat.collect do |x|
            (if x.include? "=" then x.split("=",2) else [ x, "" ] end )
          end
          collected.each do |key_value|
            key = key_value[0]
            value = key_value[1]

            # Store all keys per pack element in slurm/N/key where N is offset
            if not pack_offset.nil?
              job['slurm/offset_#{pack_offset}/#{key}']=value
            end

            # Store a second copy in the slurm/pack folder.  For this,
            # we use the first copy of a key we find, but allow offset
            # 0 to override.
            slurm_key="slurm/pack/#{key}"
            if pack_offset.nil? or pack_offset==0
              job[slurm_key]=value
            elsif not job.include? slurm_key
              job[slurm_key]=value
            end
          end # collect key/value pairs
        end # each line of stdout
        return jobs
      end # text_to_jobs

      def process_slurm_info(jobs)
        jobs.each do |pack_id,job|
          if job.nil?
            raise "Nil job for job id #{pack_id}"
          end
          job['long_state']=job.fetch('slurm/pack/JobState','??')
          if STATE_MAP.include? job['long_state']
            job['state']=STATE_MAP[job['long_state']]
          else
            job['state']=job['long_state']
          end
          job['queue']=job.fetch('slurm/pack/QOS','??')
          job['class']=job['queue']
          job['exeguess']='??'
          job['project']=job.fetch('slurm/pack/Account','??')
          job['name']=job.fetch('slurm/pack/JobName','??')
          job['out']=job.fetch('slurm/pack/StdOut','??')
          job['err']=job.fetch('slurm/pack/StdErr','??')
          job['in']=job.fetch('slurm/pack/StdIn','??')
          job['workdir']=job.fetch('slurm/pack/WorkDir','??')
          job['group']=job.fetch('slurm/pack/GroupId','??')
          if job.include? 'slurm/pack/SubmitTime'
            job['qtime']=fromdate(job['slurm/pack/SubmitTime'])
          else
            job['qtime']='??'
          end

          if job.include? 'slurm/pack/UserId'
            user_name_id=job['slurm/pack/UserId']
            if user_name_id =~ /([^\(]+)\((\S+)\)/
              job['user']=$1
              job['uid']=$2
            else
              job['user']=user_name_id
            end
          end

          if job.include? 'slurm/pack_offset_list'
            ncpu=0
            job['slurm/pack_offset_list'].each do |pack_offset|
              ncpu_key="slurm/offset_#{pack_offset}/NumCPUs"
              ncpu = ncpu + job.fetch(ncpu_key,0).to_i
              job['procs_from']='Sum of NumCPUs over entire pack.'
            end
          else
            ncpu=job.fetch('slurm/pack/NumCPUs',nil)
            job['procs_from']='NumCPUs'
          end
          if ncpu.nil?
            job['procs']='??'
            job['procs_from']='Gave up.  Have no NumCPUs.'
          else
            job['procs']=ncpu
          end
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
