$haveyaml=false
begin
  require 'yaml'
  $haveyaml=true
rescue LoadError
  #warn "Your ruby installation is broken: YAML is not installed correctly.  Continuing without YAML."
end

require 'fileutils'
require 'emc/nonil.rb'

module EMC
  module Queues

    ########################################################################
    ## CLASS QueueState ####################################################
    ########################################################################

    # This is the parent class of all queue state monitoring classes.  A
    # QueueState object stores a hash mapping from some unique job
    # identifier ("jobid") to a set of attributes.  The attributes are
    # stored in a hash as well.  Certain pre-defined attributes exist on
    # ALL platforms, whereas platform-specific attributes begin with str/
    # where "str" varies from platform to platform.
    #
    # Cross-platform attributes:
    #
    # jobid -- unique job identifier
    # out -- stdout location, which may be relative, blank if unknown
    # err -- stderr location, which may be relative, blank if unknown
    # workdir -- job inital working directory
    # order -- index in which this appeared in the list of jobs
    # name -- job name
    # account -- to whoom the resource usages are charged
    # user -- user who owns this job
    # queue -- the queue in which this job resides
    # state -- one or two letter job state
    # long_state -- only present on some platforms.  This is the long
    #             string from which the one- or two-letter state was found
    #             ("Batch Hold" = BH, "Running" = R, etc.)
    # group -- job group (subaccount)
    # class -- job class (often same as queue)
    # exeguess -- guess as to the executable location, which cannot always
    #             be found on some platforms
    # procs -- guess as to the number of processors used or "??" if unknown

    class QueueState
      include EMC::NoNil
      attr_accessor :user,:opts,:jobs
      attr_accessor :queue_from,:queue_age_type,:queue_time
      def initialize(options,user)
        @opts=options
        if(@opts==nil) then
          raise 'nil opts'
        end
        @user=user
        @jobs={}
        @queue_from="no data source"
        @queue_age_type="never initialized"
        @queue_time=Time.new()
      end
      def size()
        return 0 if @jobs.nil?
        return jobs.size
      end
      def each()
        @jobs.each { |key,value|
          yield key,value
        }
      end
      def jobids()
        return @jobs.keys
      end
      def user()
        return @user
      end
      def non_yaml_dump(hash_of_hashes)
        out=''
        hash_of_hashes.each { |jobid,jobhash|
          if(jobhash==nil) then
            raise "#{jobid} has a nil hash"
          end
          out+="JOBID #{jobid} CONTAINS\n"
          jobhash.each { |key,value|
            out+="#{key} = #{value}\n"
          }
        }
        return out
      end
      def non_yaml_load(text)
        out={}
        jobid=nil
        job={}
        text.each_line { |line|
          if(line=~/^JOBID (.*?) CONTAINS$/) then
            if(jobid!=nil) then
              out[jobid]=job
            end
            jobid=$1
            job=Hash.new()
          elsif(line=~/^([^=]*?) = (.*)$/) then
            job[$1]=$2
          end
        }
        if(jobid!=nil) then
          out[jobid]=job
        end
        return out
      end
      def update()
        result=read_cache_file
        if(result==nil) then
          #puts "need to get result from queue manager"
          @queue_from="call_queue_manager"
          @queue_age_type="run at"
          @queue_time=Time.new()
          result=call_queue_manager()
          if(result!=nil && cache_result?) then
            #puts "need to cache result"
            if($haveyaml) then
              cache_result(YAML.dump(result))
            else
              warn "Trying to cache result without YAML since your Ruby installation is broken." if @opts.verbose
              cache_result(non_yaml_dump(result))
            end
            #puts "have cached result"
          else
            #puts "either got no result or did not need to cache result, so no caching was done"
          end
        else
          #puts "got result from cache file"
        end
        if(result==nil)
          #puts "nil result"
          @jobs={}
        else
          #puts "non-nil result"
          @jobs=result
        end
      end
      def cache_result?()
        return (!@opts.disable_caching && @opts.auto_update)
      end
      def cache_result(data)
        #puts 'entered cache_result()'
        cache_full_file=cache_file()
        cache_temp_file=cache_full_file+sprintf("%06d-%06d-%06d",rand(1000000),rand(1000000),rand(1000000))
        begin
          dir_of=File.dirname(cache_temp_file)
          if(!dir_of.nil? && dir_of!='' && !File.exists?(dir_of))
            FileUtils.mkdir_p(dir_of)
          end
        end
        begin
          #puts "Write to #{cache_temp_file}"
          f=File.open(cache_temp_file,'w')
          f.write(data)
          f.close()
          #puts "wrote."
        rescue Exception=>e
          warn "#{cache_temp_file}: trouble writing to file: #{e.message}"
        end
        #puts `ls -l #{cache_temp_file}`
        begin
          #puts "Move #{cache_temp_file} to #{cache_full_file}"
          FileUtils.mv(cache_temp_file,cache_full_file)
          #puts "moved."
        rescue
          warn "#{cache_temp_file}: cannot move to #{cache_full_file}"
          begin
            FileUtils.rm(cache_temp_file)
          rescue
            warn "#{cache_temp_file}: cannot delete"
          end
        end
        #puts `ls -l #{cache_full_file}`
      end
      def read_cache_file()
        result=nil
        cache_full_file=cache_file()
        now=Time.new
        #puts "Now: #{now}"

        if(cache_full_file==nil) then
          #puts 'nil from cache_file()'
          return result
        end

        if not File.exist?(cache_full_file) and not @opts.cache_file.nil? \
          and File.exist?(@opts.cache_file)
          cache_full_file=@opts.cache_file
        end
        
        if(File.exist?(cache_full_file)) then
          oldtime=File.mtime(cache_full_file)
          @queue_from=cache_full_file
          @queue_age_type="last modified"
          @queue_time=oldtime
          #puts "#{cache_full_file} exists with mtime #{oldtime}."
          age=now-oldtime
          if(age<=@opts.max_age || @opts.only_cache) then
            #puts "  ... #{age} is within #{@opts.max_age} or only_cache is true "
            begin
              f=File.open(cache_full_file,'r')
              fromloc=cache_full_file
              agetype='last modified'
              result=f.read()
              f.close()
              if($haveyaml) then
                result=YAML.load(result)
              else
                warn "Trying to load cached data without YAML since your Ruby installation is broken." if @opts.verbose
                result=non_yaml_load(result)
              end
              #puts "  ... was able to read"
            rescue
              #puts "  ... trouble reading"
              result=nil
              begin
                f.delete()
              rescue
              end
              if(@opts.only_cache)
                raise "Cache file \"#{cache_full_file}\" could not be parsed, and you disabled running of qstat (-N or --force-caching) so I cannot determine the queue contents."
              end
            end
          else
            #puts "  ... #{age} is not within #{@opts.max_age} and only_cache is false"
          end
        elsif(@opts.only_cache)
          raise "Cache file \"#{cache_full_file}\" does not exist, and you disabled running of qstat (-N or --force-caching) so I cannot determine the queue contents."
        end
        return result
      end
      def cache_file()
        if(@opts.cache_file!=nil && !@opts.disable_caching)
          cache_full_file=@opts.cache_file+'_qq'
          if(@user==nil) then
            cache_full_file+='ALL'
          else
            cache_full_file+='u_'+@user
          end
          cache_full_file+='__no_done' if @opts.no_complete
          warn "cache file is \"#{cache_full_file}\"" if @opts.verbose
          return cache_full_file
        else
          return nil
        end
      end
      def call_queue_manager()
        raise 'not implemented; use a subclass'
      end

    end


  end
end
