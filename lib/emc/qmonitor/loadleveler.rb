require 'emc/nonil'
require 'emc/qmonitor/queuestate.rb'

module EMC
  module Queues
    ########################################################################
    ## CLASS LoadLevelerQueueState #########################################
    ########################################################################

    class LoadLevelerQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def call_queue_manager()
        result=nil
        user=@user
        
        if(!@opts.only_cache) then
          command="#{@opts.llq_path} -l"
          if(@opts.manual_options!=nil)
            command+=" #{@opts.manual_options}"
          elsif(user!=nil) then
            command+=" -u #{user}"
          end
          warn "COMMAND: #{command}" if @opts.verbose
          @queue_from=command
          jobs=Hash.new()
          reps=@opts.reps

          if(reps==nil || reps<1) then
            reps=1
          end
          for irep in 1..reps
            jobs=llq_parse(`#{command}`)
            if(irep<@opts.reps && @opts.rep_sleep>0) then
              sleep(@opts.rep_sleep)
            end
          end
        end
        if(jobs.empty?) then
          return nil
        else
          return jobs
        end
      end
      def insert_job(jobid,job,job_list,counter)
        if(!job.empty? && jobid!=nil && jobid!='') then
          # Parse out cross-platform information:
          job['index']=counter
          job['account']=job['ll/account']
          job['group']=job['ll/group']
          job['class']=job['ll/class']
          job['queue']=job['ll/class']
          job['user']=job['ll/owner']
          job['workdir']=job['ll/initial_working_dir']
          job['out']=job['ll/out']
          job['err']=job['ll/err']
          job['name']=job['ll/job_name']
          begin
            job['qtime']=Time.parse(job['ll/queue_date']).to_i
          rescue
            job['qtime']=0
          end
          job['procs']=job['ll/num_task_inst']
          job['exeguess']=job['ll/cmd']
          job['long_state']=job['ll/status']
          job['state']=@opts.namemap[nonil(job['long_state'])]
          if(job['state']==nil || job['state']=='' || job['state']=~/\A\s*\z/ ) then
            job['state']=job['long_state']
          end

          # Insert the job into the hash:
          job['jobid']=jobid
          job_list[jobid]=job
        end
        return job_list
      end
      def llq_parse(text)
        counter=0
        jobid=nil
        job={}
        job_list={}
        text.each{ |line|
          if(line=~/^\s*===== Job Step (\S+) =====\s*$/) then
            new_jobid=$1
            counter+=1
            insert_job(jobid,job,job_list,counter)
            jobid=new_jobid.gsub(/.(ncep|rdhpcs).noaa.gov/,'');
            job={"ll/full_jobid"=>new_jobid}
          elsif(line=~/^\s*([A-Za-z0-9_ \t]+?)\s*:\s*(.*)\s*$/) then
            varname=$1
            value=$2
            varname=varname.gsub(/[^A-Za-z0-9_]/,'_').downcase
            #puts "#{jobid}: ll/#{varname}=#{value}"
            job["ll/#{varname}"]=value
            job["ll/LINE_#{varname}"]=line.gsub(/\A\s*/,'').gsub(/\s*\z/,'')
          end
        }
        insert_job(jobid,job,job_list,counter)
        return job_list
      end
    end
  end
end
