require 'rexml/namespace'
require 'rexml/document'
require 'open3'
require 'emc/qmonitor/queuestate.rb'
require 'emc/nonil.rb'

module EMC
  module Queues

    ########################################################################
    ## CLASS LSFQueueState #################################################
    ########################################################################

    def intjobval(job,str)
      val=job[str]
      return nil if val.nil?
      return nil if val==''
      return nil unless /^\d+$/.match(val)
      return val.to_i
    end

    def timejobval(job,str)
      val=intjobval(job,str)
      return nil if val.nil?
      return nil if val<100000
      return val
    end
    
    def strjobval(job,str)
      val=job[str]
      return nil if val.nil?
      return nil if val==''
      return nil if /^\s*$/.match(val)
      return val
    end
    
    class LSFQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def for_time(time)
        time=time.to_i
        ft=LSFQueueState.new(@opts,@user)
        @jobs.each do |jobid,job|
          submit_time=timejobval(job,'lsf/time/Submitted')
          dispatch_time=timejobval(job,'lsf/time/Dispatched')
          start_time=timejobval(job,'lsf/time/Started')

          next if not submit_time.nil? and submit_time>time
          next if not dispatch_time.nil? and dispatch_time>time
          next if not start_time.nil? and start_time>time

          state=nil
          native_state=nil
          found_events=false
          #puts "#{jobid}: #{job['lsf/events'].length} events"
          job['lsf/events'].each do |etime,head,accum|
            if etime.nil?
              puts "ERROR: #{jobid}: NIL ETIME FOR HEAD #{head} ACCUM #{accum}"
              next
            end
            if etime==''
              puts "ERROR: #{jobid}: ETIME IS EMPTY FOR HEAD #{head} ACCUM #{accum}"
              next
            end
            if etime<100000
              puts "ERROR: #{jobid}: BAD ETIME #{etime} FOR HEAD #{head} ACCUM #{accum}"
              next
            end
            if etime>time
              #puts "#{jobid}: stopping at event #{head} #{accum}"
              break
            end
            found_events=true
            case head
            when 'Submitted','Job','Dispatched','Pending'
              state='Q'
              native_state='QUEUED'
            when 'Starting','Started'
              state='ER'
              native_state='ErroneousRunning'
            when 'reservation_id'
              state='R'
              native_state='Running'
            when 'Exited','Completed','Done'
              state='C'
              native_state='Completed'
            when 'Suspended'
              state='H'
              native_state='Held'
            when 'Signal'
            else
              puts "ERROR: #{jobid}: UNRECOGNIZED HEAD #{head}"
            end
          end # each event
          if state=='R'
            # Is it actually ZR?
            res_time=timejobval(job,'lsf/time/reservation')
            age=time-res_time
            runlimit=strjobval(job,'lsf/runlimit').to_f
            zombie_limit=runlimit*60.0+@opts.running_zombie_age.to_i
            if age>zombie_limit
              state='ZR'
              native_state='ZombieRunning'
            end
          elsif state=='C'
            done_time=timejobval(job,'lsf/time/Done')
            if not done_time.nil?
              native_state='Done'
            end
            exit_code=intjobval(job,'lsf/exit_code')
            if not exit_code.nil? and exit_code!=0
              native_state='NonZeroExit'
              state='RM'
            end
            term_reason=strjobval(job,'lsf/term_reason')
            if not term_reason.nil?
              native_state=term_reason
              state='RM'
            end
          end # if R or C state
          if not found_events
            # job does not start until after this time.
          elsif not state.nil?
            newjob=job.clone
            newjob['lsf/events']=job['lsf/events'].clone
            newjob['state']=state
            newjob['native_state']=native_state
            ft.jobs[jobid]=newjob
            #puts "#{jobid}: state=#{state} native=#{native_state}"
          else
            puts "#{jobid}: no state: #{job}"
          end # if need to store job
        end # each job
        return ft
      end # for_time
      
      def call_queue_manager()
        result=nil
        user=@user
        
        if(!@opts.only_cache) then
          if ! @opts.bhist_options.nil?
            job_list_command="#{@opts.bhist_path} #{@opts.bhist_options} -l -a "
          else
            job_list_command="#{@opts.bjobs_path} -l -X "
          end
            if(@opts.manual_options!=nil)
              job_list_command+=" #{@opts.manual_options}"
            elsif(user==nil) then
              job_list_command+=" -u all"
            else
              job_list_command+=" -u #{user}"
            end
            
            warn "#{job_list_command}" if @opts.verbose
            agetype='AT'
            jobs=Hash.new()
            reps=@opts.reps
            if(reps==nil || reps<1) then
              reps=1
            end
            @queue_from=job_list_command
            for irep in 1..reps
              result=''
              job_types=['-s','-p','-x',' ']
              if(!@opts.no_complete) then
                job_types.push('-d')
              end
              for job_type in job_types
                tz=ENV['TZ']
                ENV['TZ']="UTC"
                stdin,stdout,stderr = Open3.popen3("#{job_list_command} #{job_type}")
                ENV['TZ']=tz
                result+=stdout.read()
              end
              # puts "LIST RESULT: \n#{result}\n(end of list result dump)";
              text2jobs(result,jobs,'lsf')
              if(irep<@opts.reps && @opts.rep_sleep>0) then
                sleep(@opts.rep_sleep)
              end
            end
        end
        if(jobs.empty?) then
          # puts "jobs empty so return nil"
          return nil
        else
          # puts "return jobs"
          return jobs
        end
      end

      def text2jobs(text,hat,prepend)
        mode='top' # stores parser mode
        head=nil   # when mode=='multiline', stores the type of data
        accum=''   # used to accumulate multi-line fields
        jobid=nil  # current job id
        job=nil    # hash with current job's info
        cannot_reuse=nil # number of nodes that cannot be reused
        date=nil   # datestamp of this piece of metadata (nil for job header)
        runlimit=nil
        events=Array.new
        # puts "START OF PARSING"
#        hat['CRAYLINUX']='false'
        text.each_line { |line|
          if(line=~/^ *PGID: \d+\s*;\s+PIDs:/)
            # puts "SKIP PGID/PID LINE: #{line}"
            next
          end
          if(mode=='multiline')
            matches=/^ {20,26}(.+)/.match(line)
            if(matches)
              # This is yet another line in a multiline block, so accumulate it.
              # puts "APPEND: #{matches[1]}"
              accum+=matches[1]
              next
            else
              # We've reached the end of the multiline block, so
              # process the accumulated text, and then reprocess the
              # current line in parser mode "top".
              jobid,job = block2info(accum,head,date,jobid,job,events)
              mode='top'
              date=nil
            end
          end

          if(line=~/Recently released .* cannot be re-used at this moment: (\d+)/)
            cannot_reuse=$1.to_i
            #puts("RUNLIMIT = #{runlimit}")
          elsif(line=~/^\s*(\d+(?:\.\d*)?) min of/)
            runlimit=$1.to_f
            #puts("RUNLIMIT = #{runlimit}")
          elsif(line=~/^\s*Job *<(\d+)>/)
            # new job found.  Example:
            # Job <67340>, Job Name <t878_72hr_1>, User <ibmatp>, Project <default>, Status <
            #                           RUN>, Queue <hpc_ibm>, Command <#! /bin/bash;#BSUB -a
            #                            poe;#BSUB -J t878_72hr_1;#BSUB -n 448;#BSUB -R span[
            #                           ptile=4];#BSUB -x;#BSUB -o t878.72hr_1.stdout.%J;#BSU
            #puts "NEW JOB: #{line}"
            accum=line.chomp
            mode='multiline'
            head='Job'
            if(!jobid.nil?) # store the old job if this isn't the first
              # puts "STORE #{jobid}"
              job['lsf/runlimit']=runlimit.to_s if(!runlimit.nil?)
              job['lsf/cannot_reuse']=cannot_reuse.to_s if(!cannot_reuse.nil?)
              job['lsf/events']=events
              hat[jobid]=finishParsingJob(jobid,job)
              jobid=nil
              job=nil
              date=nil
              events=Array.new
            end
          else
            # Not a new job, but a new multiline block of metadata
            #
            # Example:
            # Wed Oct 31 15:24:26 2012: Submitted from host <t1c32f>, CWD </gpfs/td1/ibm/ibma
            #                           tp/72hr/scripts>, Output File <t878.72hr_1.stdout.%J>
            #                           , Error File <t878.72hr_1.stderr.%J>, Exclusive Execu
            #                           tion, 448 Processors Requested, Requested Resources <
            #                           span[ptile=4]>;
            #
            matches=/^([A-Z][a-z][a-z] [A-Z][a-z][a-z] [ 0-9]\d [ 0-9]\d:\d\d:\d\d(?: \d\d\d\d)?): ([A-Za-z_]+)(.*)/.match(line)
            if(matches)
              date=matches[1]
              type=matches[2]
              if(type=='Submitted' || type=='Started' || type=='Resource' \
                 || type=='reservation_id' || type=='Dispatched' \
                 || type=='Exited with' || type=='Completed' \
                 || type=='Suspended' || type=='Pending' || type=='Signal' \
                 || type=='Resource' || type=='Done' || type=='Starting')
                mode='multiline'
                head=type
                accum=matches[2].to_s+matches[3]
                #puts "IS A #{head} MULTILINE START: #{line}"
              else
                #puts "IGNORING (bad type '#{type}'): #{line}"
              end
            else
              # puts "IGNORING (not date/type header): #{line}"
            end
          end
        }
        if(mode=='multiline')
          if head=='Job'
            runlimit=nil
            cannot_reuse=nil
            #puts("RUNLIMIT RESET FOR JOB #{jobid}")
          end
          jobid,job = block2info(accum,head,date,jobid,job,events)
        end
        if(!jobid.nil? && !job.nil?)
          #puts "STORE #{jobid} AT END"
          job['lsf/events']=events
          job['lsf/cannot_reuse']=cannot_reuse.to_s if(!cannot_reuse.nil?)
          job['lsf/runlimit']=runlimit.to_s if(!runlimit.nil?)
          hat[jobid]=finishParsingJob(jobid,job)
        end

        # puts "END OF PARSING"
        return hat
      end

      def finishParsingJob(jobid,job)
        cwd=job['lsf/submissionCWD']
        cwd=job['lsf/executionCWD'] if cwd.nil?
        if job['state']=='R' && job['lsf/CRAYLINUX']=='true'
          if job['reservation'].nil? || job['reservation']==''
            job['long_state']='ErroneousRunning'
            job['state']='ER'
          end
          if not job['lsf/runlimit'].nil? and \
             not job['lsf/time/reservation'].nil?
            age=Time.new.to_i-job['lsf/time/reservation'].to_i
            runlimit=job['lsf/runlimit'].to_i*60
            if age > runlimit+@opts.running_zombie_age.to_i
              #puts "Zombie #{job['jobid']} age=#{age} runlimit+6hr=#{runlimit+6*3600}"
              job['long_state']='ZombieRunning'
              job['state']='ZR'
            else
              #puts "Job #{job['jobid']} age=#{age} runlimit+6hr=#{runlimit+6*3600}"
            end
          else
              #puts "Job #{job['jobid']} runlimit=#{job['lsf/runlimit']} res=#{job['lsf/time/reservation']}"
          end
        else
          #puts "Job #{job['jobid']} state=#{job['state']} craylinux=#{job['lsf/CRAYLINUX']} extsched=#{job['lsf/Extsched']}"
        end
        job['workdir']=cwd
        return job
      end

      def block2info(accum,head,date,jobid,job,events)
        # puts "BLOCK2INFO: accum=(#{accum}) head=#{head} date=#{date} jobid=#{jobid}"
        job={} unless job
        events=List.new unless events
        date=fromdate(date)
        if not date.nil? and not head.nil? and date.to_i>100000 and head!=''
          #puts "STORE BLOCK: accum=(#{accum}) head=#{head} date=#{date} jobid=#{jobid}"
          events << [ date.to_i,head,accum ]
        else
          #puts "BAD BLOCK: accum=(#{accum}) head=#{head} nil?=#{head.nil?} date=#{date}=#{date.to_i} nil?=#{date.nil?} jobid=#{jobid}"
        end
        case head
        when 'Job'
          # puts "IN JOB"
          jobid=reggrab(/Job *<([^>]*)>/,accum)
          return nil,nil unless jobid
          job['jobid']=jobid
          job['name']=reggrab(/Job Name *<([^>]+)>/,accum)
          job['user']=reggrab(/User *<([^>]+)>/,accum)
          status=reggrab(/Status *<([^>]+)>/,accum)
          if(status.nil? || status=='')
            status='??'
          end
          job['long_state']=status
          # job['state']=status[0..1]
          job['state']=@opts.namemap[nonil(job['long_state'])]
          # puts "Job #{jobid} long_state #{status} state #{job['state']}"
          job['project']=reggrab(/Project *<([^>]+)>/,accum)
          job['group']=job['project']
          job['queue']=reggrab(/Queue *<([^>]+)>/,accum)
          job['class']=job['queue']
          if(accum =~ /, Interactive[a-z A-Z-]*,/)
            job['lsf/interactive']='yes'
          else
            job['lsf/interactive']='no'
          end
          job['lsf/command']=reggrab(/Command *<(.*)>/,accum)
          job['lsf/Extsched']=reggrab(/Extsched *<([^>]*)>/,accum)
          if not job['lsf/Extsched'].nil? and job['lsf/Extsched'].include? "CRAYLINUX[" then
            job['lsf/CRAYLINUX']='true'
          else
            job['lsf/CRAYLINUX']='false'
          end
        when 'reservation_id'
          job['reservation']=reggrab(/=\s*([A-Za-z0-9._]+)\s*;/,accum)
          job['lsf/time/reservation']=date
          #puts "#{job['jobid']} res time #{job['lsf/time/reservation']}"
        when 'Submitted'
          if job['lsf/time/Submitted'].nil?
            #warn "fromdate(#{date})=#{job['qtime']}"
            job['lsf/time/Submitted']=job['qtime']
          end
          # puts "IN SUBMITTED"
          job['host']=reggrab(/host *<([^>]+)>/,accum)
          job['qtime']=date
          subcwd=nonil(expand_path(job,reggrab(/CWD *<([^>]+)>/,accum)))
          job['lsf/submittedCWD']=subcwd
          if(job['lsf/interactive']=='yes') then
            job['out']='(interactive)'
            job['err']='(interactive)'
          else
            job['out']=pathify(subcwd,expand_path(job,reggrab(/Output File[^<]*<([^>]+)>/,accum)))
            job['err']=pathify(subcwd,expand_path(job,reggrab(/Error File[^<]*<([^>]+)>/,accum)))
          end
          job['procs']=intgrab(/(\d+) Processors Requested/,accum)
          s=accum.scan(/(\d+)\*\{select\[craylinux/)
          if s.empty?
            job['lsf/span/ptile']=intgrab(/Requested Resources *<[^>]*span\[[^\]\>]*ptile=(\d+)\][^>]*>/,accum)
          else
            nprocs=0
            s.each { |match|
              nprocs += match[0].to_i
            }
            job['procs']=nprocs.to_s
          end
        when 'Dispatched'
          if job['lsf/time/Dispatched'].nil?
            job['lsf/time/Dispatched']=date
          end
          if job['procs'].nil?
            job['procs']=intgrab(/(\d+) Task\(s\)/,accum)
          end
          job['lsf/executionCWD']=expand_path(job,reggrab(/CWD[^<]*<([^>]+)>/,accum))
        when 'Starting'
          if job['lsf/time/Started'].nil?
            job['lsf/time/Started']=date
          end
        when 'Started'
          # puts "IN STARTED"
          if job['procs'].nil?
            job['procs']=intgrab(/(\d+) Task\(s\)/,accum)
          end
          job['lsf/time/Started']=date
          job['lsf/executionCWD']=expand_path(job,reggrab(/CWD[^<]*<([^>]+)>/,accum))
          # puts "execution CWD = ((#{job['lsf/executionCWD']}))"
          job['home']=reggrab(/Execution Home *<[^>]+>/,accum)
        when 'Exited'
          job['lsf/time/Exited']=date
          job['lsf/exit_code']=reggrab(/Exited with exit code (\d+)/,accum)
        when 'Completed'
          job['lsf/time/Completed']=date
          if /Completed <exit>; (TERM_[A-Z]+)/.match(accum)
            job['lsf/term_reason']=$1
          elsif /Completed <exit>;\s*$/.match(accum)
            job['lsf/exit_code']='0'
          end
        when 'Done'
          job['lsf/time/Done']=date
        when 'Suspended'
          job['lsf/time/Suspended']=date
          job['lsf/suspended']=reggrab(/(Suspended.*) *$/,accum)
        when 'Pending'
          job['lsf/time/Pending']=date
          job['lsf/pending']=reggrab(/Pending:? *(.*) *$/,accum)
        when 'Signal'
          job['lsf/time/Signal']=date
          job['lsf/signal']=reggrab(/Signal *<([^>]+)>/,accum)
        when 'Resource'
          # puts "IN RESOURCE"
          job['lsf/time/ResourceUsageCollected']=date
          job['lsf/used/cputime'],job['lsf/used/cputime_units'] = 
            unitgrab(/The CPU time used is (\d+) ([A-Za-z]+)/,accum)
          job['lsf/used/mem'],job['lsf/used/mem_units'] = 
            unitgrab(/MEM: (\d+) ([a-zA-Z]+)/,accum)
          job['lsf/used/swap'],job['lsf/used/swap_units'] = 
            unitgrab(/MEM: (\d+) ([a-zA-Z]+)/,accum)
          job['lsf/used/nthread']=intgrab(/NTHREAD: (\d+)/,accum)
        end
        return jobid,job
      end

      def pathify(dir,component)
        dir=pathify_helper(dir,component)
        return component if component=~/^\//
        return dir if dir.nil?
        return dir.gsub(/\/+/,'/')
      end

      def pathify_helper(dir,component)
        return nil if(component.nil?)
        return component if(dir.nil? || dir=='' || component=='')
        return component if(component[0]=='/' || component[0]=='$' || component[0]=='~')

        return "#{dir}#{component}" if dir[-1]=='/'
        return "#{dir}/#{component}"
      end

      def expand_path(jobinfo,str)
        return str if(jobinfo.nil?)
        return nil if(str.nil?)

        str=str.gsub(/((?:^|[^%])(?:%%)*)(%[jJ])/,"\\1#{jobinfo['jobid']}")
        if(str=~/HOME/) then
          if(!str=~/((?:^|[^\\])(?:[\\][\\])*)(\$HOME)/) then
            fail "DOES NOT MATCH: #{str}"
          end
        end
        str=str.gsub(/((?:^|[^\\])(?:[\\][\\])*)(\$HOME)/,"\\1#{File.expand_path('~'+jobinfo['user'])}")
        return str
      end

      def fromdate(date)
        begin
          return ((DateTime.parse(date)-DateTime.parse("1970-01-01T00:00:00+00:00"))*24*3600).to_i.to_s
          # Simpler code for ruby 1.9: 
          # return DateTime.parse(date).to_time.to_i.to_s
        rescue
          return nil
        end
      end

      def intgrab(field,text)
        str=reggrab(field,text)
        return nil if str.nil?
        begin
          return str.to_i.to_s
        rescue
          return nil
        end
      end

      def reggrab(reg,text)
        m=reg.match(text)
        if(!m.nil?)
          return m[1]
        else
          return nil
        end
      end

      def unitgrab(reg,text)
        m=reg.match(text)
        begin
          if(!m.nil? && m.length==3)
            return m[1].to_i.to_s,m[2]
          else
            return nil,nil
          end
        rescue
          return nil,nil
        end
      end
    end
  end
end
