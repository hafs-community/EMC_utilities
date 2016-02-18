require 'rexml/namespace'
require 'rexml/document'
require 'emc/nonil'
require 'emc/qmonitor/queuestate.rb'

module EMC
  module Queues
    ########################################################################
    ## CLASS TorqueQueueState ##############################################
    ########################################################################

    class TorqueQueueState < QueueState
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
            if(@opts.manual_options!=nil)
              job_list_command+=" #{@opts.manual_options}"
            elsif(user==nil) then
              # Don't need anything -- all users selected by default
            else
              job_list_command+=" -u #{user}"
            end
            warn "COMMAND: #{job_list_command}" if @opts.verbose
            result=`#{job_list_command}`
            jobids=[]
            result.each { |line|
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
            command="#{@opts.qstat_path} -x "
            if(@opts.manual_options!=nil)
              command+=" #{@opts.manual_options}"
            end
            @queue_from="#{command} job list"
            command=command+jobids.join(' ')
            warn "COMMAND: #{command}" if @opts.verbose
            fromloc=command
            agetype='AT'

            result="<result>"+`#{command}`+"</result>"
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
      def el2job(el,hat,prepend)
        key=prepend+el.local_name()
        if(el.has_text?) then
          value=el.get_text().value()
          #puts "#{key}: value=(#{value})"
        else
          #puts "#{key}: no text"
        end
        if(key!=nil && key!='') then
          if(hat[key]!=nil) then
            hat[key]=hat[key]+" "+value
          else
            hat[key]=value
          end
        end
        #puts "hat[#{key}]=(#{value})"
        #puts "key=(#{key}) value=(#{value})"
        el.elements.each do |ele|
          el2job(ele,hat,key+'/')
        end
      end

      def doc2jobs(text,jobs)
        index=0
        if(jobs==nil) then
          jobs=Hash.new
        end
        doc=REXML::Document.new(text)
        doc.elements.each('result/Data/Job') do |el|
          hat=Hash.new
          el.each() do |elc|
            el2job(elc,hat,'t/')
          end

          if(hat['t/Resource_List/flags']=~/ADVRES:([a-zA-Z0-9_.-]+)/)
            hat['reservation']=$1.to_s
          end

          # Parse out cross-platform attributes:
          hat['account']=hat['t/Account_Name']
          hat['group']=hat['t/Account_Name']
          hat['class']=hat['t/queue']
          hat['queue']=hat['t/queue']
          hat['order']=++index
          hat['jobid']=hat['t/Job_Id']
          hat['jobid']=hat['jobid'].gsub(/\..*/,'')
          hat['user']=hat['t/Job_Owner']
          hat['workdir']=hat['t/init_work_dir']
          hat['user']=hat['t/Job_Owner'].gsub(/@.*/,'')
          hat['out']=nonil(hat['t/Output_Path']).gsub(/^[a-zA-Z0-9_.-]*:/,'')
          hat['err']=nonil(hat['t/Error_Path']).gsub(/^[a-zA-Z0-9_.-]*:/,'')
          hat['name']=hat['t/Job_Name']
          hat['qtime']=hat['t/qtime']
          hat['project']=hat['t/Account_Name']

          procs=hat['t/Resource_List/procs']
          rsize=hat['t/Resource_List/size']
          if(procs==nil||procs=='') then
            procs=0
            tprocs=hat['t/Resource_List/procs']
            nodes=hat['t/Resource_List/nodes']
            if(nodes==nil || nodes=='') then
              if(rsize!=nil && rsize!='') then
                hat['procs_from']='Used Resource_List/size'
                hat['procs']=rsize
              else
                hat['procs_from']='Gave up.  Have no procs, no nodes, no size.'
                hat['procs']='??'
              end
            elsif(nodes=~/\A(\d+)\z/) then
              hat['procs_from']='Multiplied procs by nodes'
              hat['procs']=nodes.to_i * tprocs.to_i
            else
              hat['procs_from']='Parsed N+ppn=K list'
              nodes.scan(/(\d+):ppn=(\d+)/).each do |match|
                procs+=Integer(match[0])*Integer(match[1])
              end
              hat['procs']=String(procs)
            end
          else
            hat['procs_from']='Used procs'
            hat['procs']=procs
          end

          hat['procs']='??' if hat['procs']==nil || hat['procs']==''

          hat['exeguess']=nonil(hat['t/submit_args']).gsub(/.* /,'')

          hat['state']=hat['t/job_state']
          
          exit_status=hat['t/exit_status']
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
