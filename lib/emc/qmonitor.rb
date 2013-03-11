require 'emc/nonil.rb'

require 'emc/eqmoptions.rb'

require 'emc/qmonitor/moab.rb'
require 'emc/qmonitor/lsf.rb'
require 'emc/qmonitor/torque.rb'
require 'emc/qmonitor/gridengine.rb'
require 'emc/qmonitor/loadleveler.rb'

require 'emc/qmonitor/stringevaluator.rb'

require "getoptlong"
require 'fileutils'
require 'time'
require 'pathname'

module EMC
  module Queues
    ########################################################################
    ## CLASS HomeFinder ####################################################
    ########################################################################

    # Finds the home directories of any existing user

    # class HomeFinder
    #   def initialize
    #     @homes={}
    #     Etc.passwd{ |user|
    #       next if(user.name==nil || user.name=='')
    #       @homes[user.name]=user.dir
    #     }
    #   end
    #   def [](user)
    #     @homes[user]
    #   end
    #   def each()
    #     @homes.each{ |key,value|
    #       yield key,value
    #     }
    #   end
    # end

    ########################################################################
    ## CLASS JobState ######################################################
    ########################################################################

    # This class is a wrapper around one job in a QueueState object and
    # allows more sophisticated parsing and manipulation of the per-job
    # state

    class JobState
      include EMC::NoNil
      def initialize(jobid,job,evaluator,options)
        @jobid=jobid
        @job=job
        @job={} if job==nil
        @opts=options
        @evaluator=evaluator
        @morevars=nil
      end

      def morevars(v)
        @morevars=v
        return self
      end

      # The [] function returns a job attribute, possibly also performing
      # various manipulations on it.
      #   <*5:str> returns attribute "str", trimmed to the first 5 chars
      #   <-20:str> returns attribute "str", blank-padded on the left to 
      #              20 chars, but not trimmed
      # The * requests trimming, the number is the field width, and a
      # negative number requests right justification.
      def [](str)
        (star,dash,widthc,rest)=str.scan(/\A<?(\*?)(-?)(\d+:|)?(.*)>?\z/)[0]

        # If the width was specified, remove the ":" that is attached to it,
        # and get an integer back out:
        if(widthc!=nil && widthc!='' && widthc!=':') then
          width=Integer(widthc.gsub(/:\z/,''))
        else
          width=nil
        end

        # The "dash" variable should either be a "-" if one was present,
        # or "" if not:
        dash=nonil(dash)
        
        # instr is the work variable that will be processed into a return
        # value:
        instr=''
        
        # Split up the remainder of the string into variable name and functions:
        getme=rest.scan(/[a-zA-Z0-9_\/]+|\.[a-zA-Z0-9_]+(?:\([^)]+\))?/)
        #puts("scanned for functions: #{getme.length} (#{getme})")

        # First, try the job's value of the specified variable:
        instr=@job[getme[0]]

        # If that fails, try @morevars, if we have it:
        if(instr==nil && @morevars!=nil) then
          instr=@morevars[getme[0]]
        end

        # Make sure instr is a string
        instr=nonil(instr)

        # Next, apply functions:
        while(getme!=nil && getme.length>0)
          #puts "instr=(#{instr})"
          getme.shift
          #puts "getme after shift: #{getme}"
          
          # apply any functions
          getme.each do |cmd|
            parts=cmd.scan(/\A\.([a-zA-Z0-9_]+)(?:(\()(.*)(\)))?/)[0];
            #puts "parts=(#{parts[0]})(#{parts[2]})"
            if(parts!=nil) then
              #puts "call #{parts[0]}(#{parts[2]}) on (#{instr})"
              instr=@evaluator.evaluate(instr,parts[0],parts[2],@job)
              #puts "after function, (#{instr})"
            end
          end
        end
        
        # Again, make sure instr is a string:
        instr=nonil(instr)
        
        # Now handle padding and chopping:
        if(width!=nil && width>0) then
          if(star=='*') then
            # Trim to maximum of "width" chars
            len=instr.length
            if(dash=='-') then
              # trim from right
              instr=instr[Integer([len-width,0].max)..(len-1)]
            else
              # trim from left
              instr=instr[0..Integer([len-1,width-1].min)]
            end
          end
          # Pad from left or right to "width" chars:
          instr=sprintf(sprintf("%%%s%ds",dash,width),instr)
        end

        return instr
      end
      def expand(printer)
        line=''
        if(printer!=nil) then
          printer.scan(/<[^>]*>|[^<]*/).each do |str|
            if(str=~/\A<.*>\z/) then
              # This is a special insertion command
              # <*50:out> -- trim output location to first 50 chars or right-pad with blanks to 50 chars
              # <*-50:out> -- same, but trim to the LAST 50 chars or left-pad to 50 chars
              # <10:jobid> -- pad jobid to 10 chars, extending past 10 if needed
              # <exe> -- display executable name.  Don't do any padding, clipping, etc.
              
              line=line+self[str]
            else
              # This is a simple string
              line=line+str
            end
          end
        end
        return line
      end
      def parse_hhs_extra()
        text=self['out.fullpath']
        match=text.match(/(.*)\/(\d{10})\/+(\d\d[a-zA-Z])\/(?:[^\/]*\/)?(hwrf_.*)\.out\z/)
        if(match) then
          if(match[3].downcase != @opts.hhs_stid)
            return ''
          elsif(!samefile("#{match[1]}/#{match[2]}/#{match[3]}",
                          "#{@opts.hhs_hwrfdata}/#{match[2]}/#{match[3]}"))
            return ''
          else
            return match[4]
          end
        end
        match=text.match(/.*\/(\d\d[a-zA-Z][^\/]+\/jobs\/(?:WATCHERJOB|\d{10})-(?:[^-]+)-\d+.out)/)
        if(match) then
          if(!samefile(File.dirname(text),@opts.hhs_logdir))
            return ""
          else
            return match[1]
          end
        elsif(text=~/pre_master/)
          return File.basename(text)
        else
          return ''
        end
      end

      def parse_hhs_job()
        text=self['out.fullpath']
        match=text.match(/(.*)\/(\d{10})\/+(\d\d[a-zA-Z])\/(?:[^\/]*\/)?hwrf_.*\.out\z/)
        if(match) then
          if(match[3].downcase != @opts.hhs_stid)
            return 'WRONGSTORM'
          elsif(!samefile("#{match[1]}/#{match[2]}/#{match[3]}",
                          "#{@opts.hhs_hwrfdata}/#{match[2]}/#{match[3]}"))
            return 'UNKNOWNJOB'
          end
          return 'HWRF'
        end
        match=text.match(/.*\/\d\d[a-zA-Z][^\/]+\/jobs\/(?:WATCHERJOB|\d{10})-([^-]+)-\d+.out/)
        if(match) then
          if(!samefile(File.dirname(text),@opts.hhs_logdir)) then
            return 'UNKNOWNJOB'
          end
          return match[1]
        elsif(text=~/pre_master/) then
          if(defined(@opts.hhs_kick) && @opts.hhs_kick!=nil && @opts.hhs_kick!=/\A\s*\z/) then
            match=text.match(/hwrf_pre_master_kick-[^\/]+-(\d{10})-(\d\d[a-zA-Z]).out/)
            if(match) then
              if(match[2].downcase == @opts.hhs_stid) then
                if(samefile(File.dirname(text),@opts.hhs_kick)) then
                  return "HWRF"
                end
              else
                return 'WRONGSTORM'
              end
            end
          end
          return "HWRF";
        else
          return "UNKNOWNJOB";
        end
      end

      def parse_hhs_cycle()
        text=self['out.fullpath']
        match=text.match(/(.*)\/(\d{10})\/+(\d\d[a-zA-Z])\/(?:[^\/]*\/)?hwrf.*\.out\z/)
        if(match) then
          # This appears to be related to a particular cycle
          if(match[3].downcase != @opts.hhs_stid) then
            return "WRONGSTORM"
          elsif(!samefile("#{match[1]}/#{match[2]}/#{match[3]}",
                          "#{@opts.hhs_hwrfdata}/#{match[2]}/#{match[3]}")) then
            return "UNKNOWNJOB"
          end
          return match[2];
        end
        match=text.match(/.*\/\d\d[a-zA-Z][^\/]+\/+jobs\/+(WATCHERJOB|\d{10})-[^-]+-\d+.out/)
        if(match) then
          if(!samefile(File.dirname(text),@opts.hhs_logdir)) then
            return "UNKNOWNJOB"
          end
          return match[1]
        elsif(text=~/pre_master/) then
          if(defined(@opts.hhs_kick) && @opts.hhs_kick!=nil && @opts.hhs_kick!=/\A\s*\z/)
            match=text.match(/hwrf_pre_master_kick-[^\/]+-(\d{10})-(\d\d[a-zA-Z]).out/)
            if(match) then
              if(match[2].downcase == @opts.hhs_stid) then
                if(samefile(File.dirname(text),@opts.hhs_kick)) then
                  return match[1];
                end
              else
                return "WRONGSTORM";
              end
            end
          end
          return "PRE_MASTER";
        else
          return "UNKNOWNJOB";
        end
      end
    end

    class QueueState
      # Override QueueState::[] to return a JobState object
      def [](key)
        job=@jobs[key]
        if(job==nil) then
          return JobState.new(nil,nil,@opts.string_evaluator,@opts)
        else
          return JobState.new(key,job,@opts.string_evaluator,@opts)
        end
      end
    end

    ########################################################################
    ## EQMOptions::get_state ###############################################
    ########################################################################

    # Add a new "get_state" function to EQMOptions that makes the correct queuestat subclass

    class EQMOptions
      def is_torque()
        ENV['PATH'].split(':').each {|s|
          if(File.executable?("#{s}/qstat") && s =~ /torque/)
            return true
          end
        }
        return false;
      end
      def get_state()
        qs=nil

        # Is the user forcing a specific queue manager?
        if(!@queue_manager.nil?)
          case @queue_manager.downcase
          when 'torque'
            qs=EMC::Queues::TorqueQueueState.new(self,user)
          when 'gridengine'
            qs=EMC::Queues::GridEngineQueueState.new(self,user)
          when 'moab'
            qs=EMC::Queues::MoabQueueState.new(self,user)
          when 'lsf'
            qs=EMC::Queues::LSFQueueState.new(self,user)
          when 'loadleveler'
            qs=EMC::Queues::LoadLevelerQueueState.new(self,user)
          when 'pbsquery'
            qs=EMC::Queues::TorquePBSQueryQueueState.new(self,user)
          else
            warn "Unknown queue manager #{@queue_manager}.  I will guess the correct queue manager for this machine instead."
          end
        end

        if(qs.nil?)
          # Guess which cluster you are on and make the correct
          # monitor object for it.
          if(File.exist?('/lfs1') || File.exist?('/pan2')) then
            # We're on Jet.  Are we on sJet or non-s Jet?
            if(is_torque())
              qs=EMC::Queues::TorqueQueueState.new(self,user)
            else
              qs=EMC::Queues::GridEngineQueueState.new(self,user)
            end
          elsif(File.exist?('/scratch1') && File.exist?('/scratch2')) then
            qs=EMC::Queues::TorqueQueueState.new(self,user)
          elsif(File.exist?('/lustre/fs') && File.exist?('/lustre/ltfs')) then
            qs=EMC::Queues::MoabQueueState.new(self,user)
          elsif(File.exist?('/selinux')) then
            qs=EMC::Queues::LSFQueueState.new(self,user)
          else
            # Assume CCS
            qs=EMC::Queues::LoadLevelerQueueState.new(self,user)
          end
        end

        return qs
      end
    end

  end
end
