require 'rexml/namespace'
require 'rexml/document'
require 'emc/nonil'
require 'emc/qmonitor/queuestate.rb'

require 'json'
require 'date'

module EMC
  module Queues
    ########################################################################
    ## CLASS TorqueQueueState ##############################################
    ########################################################################

    class PBSQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def call_queue_manager()
        result=nil
        user=@user

        if(!@opts.only_cache) then
          reps=@opts.reps
          if(reps==nil || reps<1) then
            reps=1
          end
          jobs=Hash.new()
          for irep in 1..reps
            job_list_command="#{@opts.qstat_path}"
            find_jobs=true
            if(@opts.manual_options!=nil)
              job_list_command+=" #{@opts.manual_options}"
            elsif(user==nil) then
              # Don't need anything -- all users selected by default
              find_jobs=false
            else
              job_list_command+=" -u #{user}"
            end
            if find_jobs
              warn "COMMAND: #{job_list_command}" if @opts.verbose
              result=`#{job_list_command}`
              jobids=[]
              result.split(/[\r\n]/).each { |line|
                begin
                  line.scan(/^ *(\d+)/).each { |match|
                    jobids.push(match[0].to_i)
                  }
                rescue
                  warn "warning: cannot parse line from qstat: #{line}"
                end
              }
              if(jobids.empty?)
                return nil
              end
            end
            command="#{@opts.qstat_path} -f -F json "
            if(@opts.manual_options!=nil)
              command+=" #{@opts.manual_options}"
            end
            if find_jobs
              @queue_from="#{command} job list"
              command=command+jobids.join(' ')
            else
              @queue_from="#{command}"
            end
            warn "COMMAND: #{command}" if @opts.verbose
            fromloc=command
            agetype='AT'

            result=`#{command}`
            if(user!=nil && user!='') then
              jobs=doc2jobs(result,jobs) do |a|
                a['user']!=nil && a['user']==user
              end
            else
              jobs=doc2jobs(result,jobs)
            end
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

      def doc2jobs(text,jobs)
        index=0
        if(jobs==nil) then
          jobs=Hash.new
        end

        # json from qstat is sometimes invalid.
        cleaned = text.gsub(/^([ 	]*"[^"]*":)(00+)(,.*)$/, '\1"\2"\3')
        
        if @opts.verbose
          doc=JSON.parse(cleaned)
        else
          begin
            doc=JSON.parse(cleaned)
          rescue
            STDERR.puts("Cannot parse json text from qstat. You probably do not want the full error message, because it is often several megabytes long. Rerun with --verbose if you really want the full error message.")
          end
        end
        if doc.nil?
          return
        elsif doc["Jobs"].nil?
          return
        end

        doc["Jobs"].each do |jobid,el|
          hat = Hash.new
          
          # can't find reservation: hat['reservation']=?????

          # Parse out cross-platform attributes:
          hat['account']=el['Account_Name']
          hat['group']=el['Account_Name']
          hat['class']=el['queue']
          hat['queue']=el['queue']
          hat['order']=++index
          hat['jobid']=jobid.gsub(/\..*/,'')
          hat['user']=nonil(el['Job_Owner']).gsub(/@.*/,'')
          hat['workdir']=el["Variable_List"]["PBS_O_WORKDIR"]
          hat['out']=nonil(el['Output_Path']).gsub(/^[a-zA-Z0-9_.-]*:/,'')
          hat['err']=nonil(el['Error_Path']).gsub(/^[a-zA-Z0-9_.-]*:/,'')
          hat['name']=el['Job_Name']
          hat['qtime']=DateTime.parse(el['qtime']).to_time.to_i
          hat['project']=el['Account_Name']

          if !el['Resource_List'].nil? and !el['Resource_List'].empty?
            hat['procs'] = el["Resource_List"]["ncpus"]
          end
          if hat['procs'].nil? and !el['resources_used'].nil?
            hat['procs'] = el['resources_used']['ncpus']
          end
          
          hat['exeguess']=nonil(hat['t/Submit_arguments']).gsub(/.* /,'')

          hat['state']=el['job_state']
          
          exit_status=el['Exit_status']
          if not exit_status.nil? and exit_status=~/^\s*\d+\s*$/
            exit_status=exit_status.to_i
            if exit_status != 0
              hat['state']="RM"
            end
          end

          keep=true
          if(block_given?) then
            keep=yield(hat)
          end

          if(keep) then
            jobs[hat['jobid']]=hat
            #puts "found job (#{hat['jobid']})"
          end
        end
        return jobs
      end
    end
  end
end
