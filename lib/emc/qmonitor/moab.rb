require 'rexml/namespace'
require 'rexml/document'
require 'emc/nonil'
require 'emc/qmonitor/queuestate.rb'

module EMC
  module Queues

    ########################################################################
    ## CLASS MoabQueueState ################################################
    ########################################################################

    class MoabQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def call_queue_manager()
        result=nil
        user=@user
        
        if(!@opts.only_cache) then
          job_list_command="#{@opts.showq_path} --xml"
          if(@opts.manual_options!=nil)
            job_list_command+=" #{@opts.manual_options}"
          elsif(user==nil) then
            # Don't need anything -- all users selected by default
          else
            job_list_command+=" -w user=#{user}"
          end

          @queue_from=job_list_command
          agetype='AT'
          jobs=Hash.new()
          reps=@opts.reps
          if(reps==nil || reps<1) then
            reps=1
          end
          for irep in 1..reps
            warn "#{job_list_command}" if @opts.verbose
            result=`#{job_list_command}`
            #puts "LIST RESULT: \n#{result}\n(end of list result dump)";
            doc2list(result,jobs)

            jobs.keys.each { |k|
              #puts "Have job (#{k}) after doc2list"
            }
            if(!jobs.keys.empty?) then
              full_command="#{@opts.checkjob_path} --xml #{jobs.keys.join(" ")}"
              warn "#{full_command}" if @opts.verbose
              result=`#{full_command}`
              #puts "DETAILED RESULT: \n#{result}\n(end of detailed result dump)";
              if(user!=nil) then
                #puts "user is not nil, so filter by users"
                jobs=doc2jobs(result,jobs) { |a|
                  a['user']!=nil && a['user']==user
                }
              else
                #puts "user is nil so get everything"
                jobs=doc2jobs(result,jobs)
              end
            end
            if(irep<@opts.reps && @opts.rep_sleep>0) then
              sleep(@opts.rep_sleep)
            end
          end
        end
        if(jobs.empty?) then
          #puts "jobs empty so return nil"
          return nil
        else
          #puts "return jobs"
          return jobs
        end
      end

      def doc2list(text,jobs)
        doc=REXML::Document.new(text)
        doc.elements.each('//queue') { |queue|
          option=nonil(queue.attribute('option')).capitalize
          queue.elements.each('job') { |job|
            jobid=job.attribute('JobID').value
            next if(jobid==nil || jobid=='')
            hat={}
            job.attributes.each { |name,value|
              next if(name==nil || name=='')
              hat["m/l/#{name}"]=value
            }

            hat['jobid']=jobid
            hat['class']=nonil(hat['m/l/Class'])
            hat['account']=nonil(hat['m/l/Account'])
            hat['qtime']=hat['m/l/SubmissionTime']
            hat['qtime']=0 if hat['qtime']==nil || hat['qtime']==''
            hat['queue']=hat['class']
            hat['long_state']=hat['m/l/State']
            nn=nonil(hat['long_state'])
            gs=nn.gsub(/([a-z])([A-Z])/,'\1 \2')
            nm=@opts.namemap[gs]
            hat['state']=nm
            if(hat['state']==nil || hat['state']=='' || hat['state']=~/\A\s*\z/ ) then
              warn "cannot parse state #{hat['long_state']}: (#{nn}) (#{gs}) (#{nm})"
              hat['state']=hat['long_state']
            end
            hat['user']=hat['m/l/User']
            hat['procs']=hat['m/l/ReqProcs']
            hat['name']=hat['m/l/JobName']
            hat['group']=hat['m/l/Group']
            
            jobs[jobid]=hat
            #puts "Found job ((#{jobid})) in listing"
          }
        }
        return jobs
      end

      def fixpath(pathin,job)
        path=nonil(pathin).gsub(/\A[^\/:\~\$]*:/,'').gsub(/\$PBS_JOBNAME/,nonil(job['name'])).gsub(/\$USER/,nonil(job['user']))
        if(job['user']==Etc.getlogin) then
          return nonil(path).gsub(/\A\$HOME/,"~")
        else
          return nonil(path).gsub(/\A\$HOME/,"~#{job['user']}")
        end
      end

      def doc2jobs(text,jobs)
        index=0
        jobs.keys.each { |k|
          if(jobs[k]==nil) then
            #puts "Job (#{k}) has a nil value at top of doc2jobs"
          else
            #puts "Have job (#{k}) with non-nil value at top of doc2jobs"
          end
        }
        if(jobs==nil) then
          #puts "No job hash.  Need to make one."
          jobs=Hash.new
        end
        doc=REXML::Document.new(text)
        doc.elements.each('//job') do |job|
          jobid=job.attribute('JobID').value
          next if(jobid==nil || jobid=='')
          hat=jobs[jobid]
          #puts "jobid=(#{jobid}) hat=(#{hat}) hat==nil = (#{hat==nil}) jobid.length=(#{jobid.length})"
          jobs.keys.each { |k|
            if(k==jobid) then
              #puts "Key (#{k}) == (#{jobid})\n"
            end
          }
          if(hat==nil) then
            #puts "CANNOT FIND ((#{jobid})) IN INTERNAL JOB HASH"
            hat={}
          else
            #puts "FOUND ((#{jobid})) IN INTERNAL JOB HASH"
          end
          job.attributes.each { |name,value|
            next if(name==nil || name=='')
            hat["m/d/#{name}"]=value
          }
          hat['out']=hat['m/d/OFile']
          hat['out']=hat['m/d/RMStdOut'] if hat['out']==nil || hat['out']==''
          hat['out']=hat['m/d/StdOut'] if hat['out']==nil || hat['out']==''
          hat['out']=fixpath(hat['out'],hat)

          haveout=(hat['out']!=nil && hat['out']!='')

          hat['err']=hat['m/d/EFile']
          hat['err']=hat['m/d/RMStdErr'] if hat['err']==nil || hat['err']==''
          hat['err']=hat['m/d/StdErr'] if hat['err']==nil || hat['err']==''
          hat['err']=nonil(hat['err']).gsub(/\A[^\/]*:/,'')
          hat['err']=fixpath(hat['err'],hat)

          haveerr=(hat['err']!=nil && hat['err']!='')

          if(haveout && !haveerr) then
            hat['err']=hat['out']
          end

          hat['exeguess']=nonil(hat['m/d/CmdFile'])
          hat['exeguess']=nonil(hat['exeguess']).gsub(/\A[^\/]*:/,'')
          hat['exeguess']=fixpath(hat['exeguess'],hat)

          hat['workdir']=nonil(hat['m/d/IWD'])
          hat['workdir']=nonil(hat['workdir']).gsub(/\A[^\/]*:/,'')
          hat['workdir']=fixpath(hat['workdir'],hat)

          hat['jobid']=jobid
          jobs[jobid]=hat
        end
        return jobs
      end
    end

  end
end
