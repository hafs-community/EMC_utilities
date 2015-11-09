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

    class LSFQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def call_queue_manager()
        result=nil
        user=@user
        
        if(!@opts.only_cache) then
            job_list_command="#{@opts.bjobs_path} -l "
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
        date=nil   # datestamp of this piece of metadata (nil for job header)
        # puts "START OF PARSING"
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
              jobid,job = block2info(accum,head,date,jobid,job)
              mode='top'
              date=nil
            end
          end

          if(line=~/^\s*Job <(\d+)>/)
            # new job found.  Example:
            # Job <67340>, Job Name <t878_72hr_1>, User <ibmatp>, Project <default>, Status <
            #                           RUN>, Queue <hpc_ibm>, Command <#! /bin/bash;#BSUB -a
            #                            poe;#BSUB -J t878_72hr_1;#BSUB -n 448;#BSUB -R span[
            #                           ptile=4];#BSUB -x;#BSUB -o t878.72hr_1.stdout.%J;#BSU
            # puts "NEW JOB: #{line}"
            accum=line.chomp
            mode='multiline'
            head='Job'
            if(!jobid.nil?) # store the old job if this isn't the first
              # puts "STORE #{jobid}"
              hat[jobid]=finishParsingJob(jobid,job)
              jobid=nil
              job=nil
              date=nil
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
            matches=/^([A-Z][a-z][a-z] [A-Z][a-z][a-z] [ 0-9]\d [ 0-9]\d:\d\d:\d\d(?: \d\d\d\d)?): ([A-Za-z]+)(.*)/.match(line)
            if(matches)
              date=matches[1]
              type=matches[2]
              if(type=='Submitted' || type=='Started' || type=='Resource')
                mode='multiline'
                head=type
                accum=matches[2].to_s+matches[3]
                # puts "IS A #{head} MULTILINE START: #{line}"
              else
                # puts "IGNORING (bad type): #{line}"
              end
            else
              # puts "IGNORING (not date/type header): #{line}"
            end
          end
        }
        if(mode=='multiline')
          jobid,job = block2info(accum,head,date,jobid,job)
        end
        if(!jobid.nil? && !job.nil?)
          # puts "STORE #{jobid} AT END"
          hat[jobid]=finishParsingJob(jobid,job)
        end

        # puts "END OF PARSING"
        return hat
      end

      def finishParsingJob(jobid,job)
        cwd=job['lsf/submissionCWD']
        cwd=job['lsf/executionCWD'] if cwd.nil?
        job['workdir']=cwd
        return job
      end

      def block2info(accum,head,date,jobid,job)
        # puts "BLOCK2INFO: accum=(#{accum}) head=#{head} date=#{date} jobid=#{jobid}"
        job={} unless job
        case head
        when 'Job'
          # puts "IN JOB"
          jobid=reggrab(/Job <([^>]*)>/,accum)
          return nil,nil unless jobid
          job['jobid']=jobid
          job['name']=reggrab(/Job Name <([^>]+)>/,accum)
          job['user']=reggrab(/User <([^>]+)>/,accum)
          status=reggrab(/Status <([^>]+)>/,accum)
          if(status.nil? || status=='')
            status='??'
          end
          job['long_state']=status
          # job['state']=status[0..1]
          job['state']=@opts.namemap[nonil(job['long_state'])]
          # puts "Job #{jobid} long_state #{status} state #{job['state']}"
          job['project']=reggrab(/Project <([^>]+)>/,accum)
          job['group']=job['project']
          job['queue']=reggrab(/Queue <([^>]+)>/,accum)
          job['class']=job['queue']
          if(accum =~ /, Interactive[a-z A-Z-]*,/)
            job['lsf/interactive']='yes'
          else
            job['lsf/interactive']='no'
          end
          job['lsf/command']=reggrab(/Command <(.*)>/,accum)
        when 'Submitted'
          # puts "IN SUBMITTED"
          job['host']=reggrab(/host <([^>]+)>/,accum)
          job['qtime']=fromdate(date)
#          warn "fromdate(#{date})=#{job['qtime']}"
          job['lsf/time/Submitted']=job['qtime']
          subcwd=nonil(expand_path(job,reggrab(/CWD <([^>]+)>/,accum)))
          job['lsf/submittedCWD']=subcwd
          if(job['lsf/interactive']=='yes') then
            job['out']='(interactive)'
            job['err']='(interactive)'
          else
            job['out']=pathify(subcwd,expand_path(job,reggrab(/Output File[^<]*<([^>]+)>/,accum)))
            job['err']=pathify(subcwd,expand_path(job,reggrab(/Error File[^<]*<([^>]+)>/,accum)))
          end
          job['procs']=intgrab(/(\d+) Processors Requested/,accum)
          job['lsf/span/ptile']=intgrab(/Requested Resources <[^>]*span\[[^\]\>]*ptile=(\d+)\][^>]*>/,accum)
        when 'Started'
          # puts "IN STARTED"
          job['lsf/time/Started']=fromdate(date)
          job['lsf/executionCWD']=expand_path(job,reggrab(/CWD[^<]*<([^>]+)>/,accum))
          # puts "execution CWD = ((#{job['lsf/executionCWD']}))"
          job['home']=reggrab(/Execution Home <[^>]+>/,accum)
        when 'Resource'
          # puts "IN RESOURCE"
          job['lsf/time/ResourceUsageCollected']=fromdate(date)
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
