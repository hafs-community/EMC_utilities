require 'rexml/namespace'
require 'rexml/document'
require 'emc/qmonitor/queuestate.rb'
require 'emc/nonil.rb'

module EMC
  module Queues

    ########################################################################
    ## CLASS GridEngineQueueState ##########################################
    ########################################################################

    class GridEngineQueueState < QueueState
      include EMC::NoNil
      def initialize(options,user)
        super
      end

      def call_queue_manager()
        result=nil
        user=@user
        
        if(!@opts.only_cache) then
          job_list_command="#{@opts.qstat_path} -xml"
          if(@opts.manual_options!=nil)
            job_list_command+=" #{@opts.manual_options}"
          elsif(user==nil) then
            job_list_command+=" -u '*'"
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
            result=`#{job_list_command}`
            #puts "LIST RESULT: \n#{result}\n(end of list result dump)";
            doc2list(result,jobs)
            
            full_command="#{@opts.qstat_path} -xml -j #{jobs.keys.join(",")}"
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

      def list2job(el,hat,prepend,name_element)
        el.elements.each('element') { |elc|
          name=nil
          elc.elements.each(name_element) { |namer|
            name=namer.get_text().value if namer.has_text?
          }
          el2job(elc,hat,prepend+namer+'/')
        }
      end

      def varlist2job(el,hat,envprepend,elname,varname,valname)
        el.elements.each(elname) { |pair|
          var=nil    ;    value=nil
          pair.elements.each() { |elc|
            if(elc.has_text?) then
              var=elc.get_text().value if elc.local_name==varname
              value=elc.get_text().value if elc.local_name==valname
            end
          }
          value='' if value==nil
          if(var!=nil && var!='') then
            hat[envprepend+var]=value
          end
        }
      end
      
      def el2job(el,hat,prepend)
        key=el.local_name()
        #puts "  el2job: key=#{key}"
        case key
        when 'JB_hard_resource_list'
          el.elements.each('element') { |elc|
            list2job(elc,hat,prepend+key+'/','CE_name')
          }
          return
        when 'JB_mail_list'
          all=[]
          el.elements.each('element') { |elc|
            who='??'
            where='??'
            elc.elements.each() { |elp|
              if(elp.has_text?) then
                who=elp.get_text().value if elp.local_name=='MR_user';
                where=elp.get_text().value if elp.local_name=='MR_host';
              end
            }
            who='??' if who==nil || who==''
            where='??' if where==nil || where==''
            all.push "#{who}@#{where}"
          }
          hat[prepend+'JB_mail_list']=all.join(', ')
          return
        when 'JB_env_list'
          varlist2job(el,hat,prepend+'env/','job_sublist','VA_variable','VA_value')
          return
        when 'JB_ja_tasklist'
          return # Skip this one -- will need to write a parser for it
        when 'JB_context'
          varlist2job(el,hat,prepend+'context/','context_list','VA_variable','VA_value')
          return
        end

        #puts "  el2job: not a special case"

        if(el.has_text?) then
          value=el.get_text().value()
          #puts "#{key}: value=(#{value})"
        else
          #puts "#{key}: no text"
        end
        if(key!=nil && key!='') then
          if(hat[prepend+key]!=nil) then
            hat[prepend+key]=hat[prepend+key]+" "+value
          else
            hat[prepend+key]=value
          end
        end
        #puts "hat[#{key}]=(#{value})"
        #puts "key=(#{key}) value=(#{value})"
        el.elements.each do |ele|
          el2job(ele,hat,prepend+key+'/')
        end
      end

      def doc2list(in_text,jobs)
        text=''
        in_text.each() { |line|
                if(line=~/<JATASK:/) then
                  # workaround for bug in grid engine
                  # ignore this particular invalid line
#            warn "IGNORE: ((#{line}))"
                else
                  text+=line
#        warn "KEEP: ((#{line}))"
                end
        }
        doc=REXML::Document.new(text)
        doc.elements.each('//job_list') { |el|
          #puts "doc2list element named #{el.local_name}..."
          hat=Hash.new()
          hat['long_state']=el.attribute('state').value
          hat['long_state'].capitalize! if hat['long_state']!=nil
          el.elements.each { |elc|
            #puts "elc element #{elc.local_name()}"
            if(elc.has_text?) then
              #puts "call el2job on elc"
              el2job(elc,hat,'ge/l/')
            end
          }
          hat['state']=hat['ge/l/state']
          hat['state'].upcase! if(hat['state']!=nil)
          hat['user']=hat['ge/l/JB_owner']
          jobid=hat['ge/l/JB_job_number']
          hat['jobid']=jobid
          hat['name']=hat['ge/l/JB_name']
          #puts "   ... jobid=#{jobid}"
          jobs[jobid]=hat if jobid!=nil && jobid!=''
        }
        return jobs
      end

      def doc2jobs(in_text,jobs)
        text=''
        in_text.each() { |line|
                if(line=~/<JATASK:/) then
                  # workaround for bug in grid engine
                  # ignore this particular invalid line
#            warn "IGNORE: ((#{line}))"
                else
                  text+=line
#        warn "KEEP: ((#{line}))"
                end
        }
        index=0
        if(jobs==nil) then
          jobs=Hash.new
        end
        doc=REXML::Document.new(text)
        doc.elements.each('detailed_job_info/djob_info/element') do |el|
          #puts "have an element"
          jobid=nil
          #puts " ... look for job id"
          el.elements.each('JB_job_number') do |elc|
            #puts " ... found a JB_job_number"
            if(elc.has_text?) then
              jobid=elc.get_text().value()
              #puts " ... found value ((#{jobid}))"
            else
              #puts " ... but it has no text"
            end
          end
          #puts "jobid is #{jobid}"
          if(jobid==nil || jobid.match(/\A\s*\Z/)) then
            #puts "invalid jobid"
            next # invalid element; has no job id, so skip it and take no action
          end
          if(jobs[jobid]==nil) then
            raise "Should never get here: job #{jobid} is not already in job list."
          end
          hat=jobs[jobid]

          el.elements.each() do |elc|
            el2job(elc,hat,'ge/d/')
          end

          # Parse out cross-platform attributes:
          # NOTE: state and long_state already have been parsed by doc2list
          hat['qtime']=hat['ge/d/JB_submission_time']
          hat['account']=hat['ge/d/JB_account']
          hat['group']=hat['ge/d/JB_group']
          hat['class']=hat['ge/d/JB_hard_queue_list/QR_name']
          hat['queue']=hat['ge/d/JB_hard_queue_list/QR_name']
          hat['order']=++index
          hat['jobid']=hat['ge/d/JB_job_number']
          hat['user']=hat['ge/d/JB_owner']
          hat['workdir']=hat['ge/d/JB_cwd']
          if(hat['workdir']==nil) then
            wd=hat['ge/d/env/__SGE_PREFIX__O_WORKDIR']
            if(wd!=nil) then
              hat['workdir']=wd
            end
          end
          hat['procs']=hat['ge/d/JB_pe_range/ranges/RN_min']
          hat['user']=hat['ge/d/JB_owner']
          hat['out']=hat['ge/d/JB_stdout_path_list/path_list/PN_path']
          hat['err']=hat['ge/d/JB_stderr_path_list/path_list/PN_path']
          hat['name']=hat['ge/d/JB_name']
          if(hat['out']==nil && hat['err']!=nil) then
            hat['out']=hat['err']
          end
          if(hat['out']!=nil && hat['err']==nil) then
            hat['err']=hat['out']
          end
          hat['err']=nonil(hat['err'])
          hat['out']=nonil(hat['out'])
          hat['workdir']=nonil(hat['workdir'])
          res=hat['ge/d/context/flags']
          if(res!=nil && res!='') then
            res=res.gsub(/^ADVRES:/,'');
            hat['reservation']=res
          else
            hat['reservation']=''
          end
          
          hat['exeguess']=nonil(hat['ge/d/JB_script_file']).gsub(/.* /,'')

          keep=true
          if(block_given?) then
            keep=yield(hat)
          end

          if(keep) then
            #puts "found job (#{hat['jobid']})"
          else
            jobs[hat['jobid']]=nil
            #puts "remove job (#{hat['jobid']})"
          end
        end
        return jobs
      end
    end

  end
end
