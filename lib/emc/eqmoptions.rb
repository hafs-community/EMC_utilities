require 'getoptlong'
require 'fileutils'
require 'time'
require 'pathname'

require 'emc/qmonitor/stringevaluator'

module EMC
  module Queues
    ########################################################################
    ## CLASS EQMOptions ####################################################
    ########################################################################

    # This class parses the home directory configuration file, environment
    # and the argument list, storing the result in a EQMOptions object.

    class EQMOptions
      include EMC::NoNil
      attr_reader :hhs_stid,:hhs_hwrfdata,:hhs_logdir,:hhs_jobid,:hhs_extra,:hhs_kick,:hhs_ens
      attr_reader :colormap,:hhs_colormap,:namemap,:groups,:emu_printers,:bjobs_path
      attr_reader :emu_colormaps,:greps,:antigreps,:cache,:qstat_path,:cache_file
      attr_reader :max_age,:max_loops,:loops,:min_sleep_time,:sleep_time,:reps
      attr_reader :looping,:rep_sleep,:max_reps,:min_rep_sleep,:force_blocking
      attr_reader :clear_screen,:user,:manual_options,:force_blocking,:emu_mode
      attr_reader :vars,:auto_update,:only_cache,:disable_caching,:nohead
      attr_reader :nofoot,:colormaps,:sort_order,:sorting,:printers
      attr_reader :showq_path,:checkjob_path,:llq_path,:verbose,:no_complete
      attr_reader :queue_manager,:pbsquery_path,:bhist_path,:bhist_options
      attr_reader :string_evaluator, :running_zombie_age

      def initialize()
        set_constants()
        set_defaults()
        parse_env()
      end

      def nofoot=(value)
        begin
          @nofoot=true&&value
        rescue
          raise "Ignoring invalid value sent to nofoot="
        end
      end
      def nohead=(value)
        begin
          @nohead=true&&value
        rescue
          raise "Ignoring invalid value sent to nohead="
        end
      end

      def printers=(value)
        begin
          @printers={}
          value.each() do |k,v|
            @printers[k]=v
          end
        rescue
          raise "When setting the printers object, you must provide a hash of strings"
        end
      end

      def colormap=(value)
        begin
          @colormap={}
          value.each() do |k,v|
            @colormap[k]=v
          end
        rescue
          raise "When setting the printers object, you must provide a hash of strings"
        end
      end

      def parse_only_dash_options(argv)
          parse_dash_options(argv);
      end

      def parse_arguments(argv)
        parse_dash_options(argv)
        if(@emu_mode=='hwrf_hhs_jobspecs.pl') then
          @emu_mode='jobspecs'
        end
        if(@emu_mode=='jobspecs') then
          parse_hhs_args(argv)
        else
          parse_other_args(argv)
        end
      end

      def parse_hhs_args(argv)
        if(argv.length<3)
          usage("hwrf_hhs_jobspecs.pl mode requires at least three arguments.  Also, don't run in this mode unless you are HHS.")
        end
        @colormap=@hhs_colormap

        if(emu_mode=='jobspecs') then
          (@hhs_stid,@hhs_hwrfdata,@hhs_logdir,@hhs_jobid,@hhs_extra,@hhs_kick)=argv
          @hhs_stid=@hhs_stid.downcase
          argv.shift(6)
          #warn "stid=#{@hhs_stid} data=#{@hhs_hwrfdata}"
          #warn "log=#{@hhs_logdir}"
          #warn "jobid=#{@hhs_jobid} extra=#{@hhs_extra}"
          #warn "kick=#{@hhs_kick}"
        end
      end
      def set_emu_mode(emu_mode)
        @emu_mode=emu_mode
      end
      def set_constants()
        # Special string that indicates nothing should be printed:
        # Mapping of long status name to short status name, copied from qac2:
        @namemap={'Completed'=>'C', 'Deferred'=>'D', 'User Hold'=>'H', 'Idle'=>'I',
          'Not Run'=>'NR', 'Not Queued'=>'NQ', 'Pending'=>'P', 'Canceled'=>'CA',
          'Running'=>'R', 'Removed'=>'RM', 'Remove Pending'=>'RP', 'Canceling'=>'CA',
          'System Hold'=>'S', 'User Hold and System Hold'=>'SH', 'Batch Hold'=>'BH',
          'Starting'=>'ST', 'Vacated'=>'V', 'Reject Pending'=>'XP', 
          'Migrated'=>'M', 'Hold'=>'H',

          # LSF long names, from the "JOB STATUS" section of the bjobs man page:
          'PEND'=>'Q',  # job is queued, not yet running
          'PSUSP'=>'H', # job was suspended (held) while pending
          'RUN'=>'R',   # job is running
          'USUSP'=>'H', # job was suspended (held) while running
          'SSUSP'=>'RM',# job has been removed due to resource limits
          'DONE'=>'C',  # job completed normally with zero exit status
          'EXIT'=>'RM', # job was removed by an admin or exited normally with non-zero status
          'WAIT'=>'W',  # job has a dependency or future requested start time
          'UNKWN'=>'UK',# job state is unknown due to an error in LSF
          'ZOMBI'=>'ZB',# job is a "zombie" job due to an error in LSF
        }
        
        # Mapping of status group name to short status name.  These status
        # groups have no meaning outside this program; they're just for
        # convenience for the colormap:
        @groups={'held'=>['D','H','SH','S'], 'refused'=>['NR','XP','RM','RP','DR','BH'],
          'future'=>['W','D','H','SH','BH','S','NQ'], 'queued'=>['I','Q','M','QW','P'],
          'done'=>['C','CA','RM'],'running'=>['R','RU']}

        @hhs_colormap={'none'=>'none','done'=>'noprint'}

        @no_complete=false # if true, don't display completed jobs
        
        @emu_printers={
          'blank'=>{ },

          'hurqac'=>{
            'default'=>'<14:id> <*-7:class> <-2:state> <*11:queue.time()> <out.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>' Job Step ID    Class  ST  Queue Time > Output File',
            'head1'=>'-------------- ------- -- -----------   --------------------------------------',
          },
          'climqac'=>{
            'default'=>'<exe.shortpath>  <15:jobid>    <*11:qtime.time()>  <2:state>',
            'head1'=>'',
            'head1'=>'  Executable             ID         Submitted    ST ',
          },
          'eqm'=>{
            'default'=>'<14:jobid> <5:procs> <-2:state> <*11:qtime.time()>   <out.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID     Procs ST Queue Time    Output Location',
            'head2'=>'-------------- ----- -- -----------   ------------------------------------'
          },
          'qerr'=>{
            'default'=>'<14:jobid> <5:procs> <-2:state> <*11:qtime.time()>   <err.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID     Procs ST Queue Time    StdErr Location',
            'head2'=>'-------------- ----- -- -----------   ------------------------------------'
          },
          'qexe'=>{
            'default'=>'<14:jobid> <5:procs> <-2:state> <*11:qtime.time()>   <exeguess.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID     Procs ST Queue Time    Executable/Script Location',
            'head2'=>'-------------- ----- -- -----------   ------------------------------------'
          },
          'qname'=>{
            'default'=>'<14:jobid> <5:procs> <-2:state> <*11:qtime.time()>   <name>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID     Procs ST Queue Time    Job Name',
            'head2'=>'-------------- ----- -- -----------   ------------------------------------'
          },

          'eqm-all'=>{
            'default'=>'<14:jobid> <*12:user> <5:procs> <-2:state> <*11:qtime.time()>   <out.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID       Username   Procs ST Queue Time    Output Location',
            'head2'=>'-------------- ------------ ----- -- -----------   ------------------------------------'
          },
          'qerr-all'=>{
            'default'=>'<14:jobid> <*12:user> <5:procs> <-2:state> <*11:qtime.time()>   <err.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID       Username   Procs ST Queue Time    StdErr Location',
            'head2'=>'-------------- ------------ ----- -- -----------   ------------------------------------'
          },
          'qexe-all'=>{
            'default'=>'<14:jobid> <*12:user> <5:procs> <-2:state> <*11:qtime.time()>   <exeguess.shortpath>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID       Username   Procs ST Queue Time    Executable Location',
            'head2'=>'-------------- ------------ ----- -- -----------   ------------------------------------'
          },
          'qname-all'=>{
            'default'=>'<14:jobid> <*12:user> <5:procs> <-2:state> <*11:qtime.time()>   <name>',
            'foot2'=>'From <Q_SOURCE> (age <Q_AGE> sec.)',
            'head1'=>'    Job ID       Username   Procs ST Queue Time    Job Name',
            'head2'=>'-------------- ------------ ----- -- -----------   ------------------------------------'
          }
        }

        @emu_printers['qout']=@emu_printers['eqm']
        @emu_printers['default']=@emu_printers['eqm']
        @emu_printers['qout-all']=@emu_printers['eqm-all']
        @emu_printers['default-all']=@emu_printers['eqm-all']
        
        @emu_colormaps={
          'blank'=>{
            'default'=>'noprint'
          },

          'default'=>{
            'default'=>'red',
            'queued'=>'magenta+bold',
            'R'=>'blue+bold',
            'future'=>'magenta',
            'foot'=>'none',
            'head'=>'bold',
            'done'=>'none'
          }
        }
      end
      def add_regexp(str,list)
        begin
          list.push(Regexp.new(str,Regexp::IGNORECASE))
        rescue Exception=>e
          # If an error occurs, do not add the regular expression to
          # the list.
          warn "#{str}: Invalid regexp (#{e})"
        rescue
          warn "#{str}: Invalid regexp (no reason specified)"
        end
      end
      def set_defaults()
    
        # Default settings:
        @hhs_ens=nil
        @running_zombie_age=600
        @greps=Array.new
        @antigreps=Array.new
        @cache=nil
        @bjobs_path='bjobs'
        @bhist_path='bhist'
        @qstat_path='qstat'
        @bhist_options=nil
        @pbsquery_path='pbsquery'
        @llq_path='llq'
        @showq_path='showq'
        @checkjob_path='checkjob'
        @cache_file=ENV['QAC2_LLQ_FILE']
        @max_age=15
        @max_loops=360
        @loops=30
        @min_sleep_time=5
        @sleep_time=60
        @reps=1
        @looping=false
        @rep_sleep=10
        @max_reps=5
        @min_rep_sleep=5
        @force_blocking=false
        @clear_screen=false
        @user=ENV['USER'];
        @manual_options=nil
        @force_blocking=false
        @emu_mode=nil
        @vars=Array.new()
        @auto_update = (ENV['QAC2_AUTOUPDATE']=='YES')
        @only_cache=false
        @disable_caching=false
        @nohead=false # true=disable printing of header
        @nofoot=false # true=disable printing of footer
        @verbose=false

        @hhs_stid=nil
        @hhs_hwrfdata=nil
        @hhs_logdir=nil
        @hhs_jobid=nil
        @hhs_extra=nil
        @hhs_kick=nil

        @string_evaluator=StringEvaluator.get_default()
        @queue_manager=nil # nil = try to guess queue manager

        # nc = true iff color and other terminal attributes are disabled:
        @nc=!STDOUT.tty?
        if(@nc) then
          @nohead=true
          @nofoot=true
        end

        # The colormap.  This is used to decide which status or status groups
        # get what colors:
        @colormap={'default'=>'none'}
        
        # The how to print job information:
        @printers={}
        
        @sort_order=[ ['s',-1,'out.shortpath'],['n',1,'order'] ];
      end
      def parse_env()
        if(ENV['QAC2_LLQ_MAX_AGE']!=nil) then
          match=ENV['QAC2_LLQ_MAX_AGE'].match(/\A([1-9]\d+)\z/)
          if(match) then
            @max_age=Integer(match[1])
          end
        end
      end
      def color_disabled()
        return @nc
      end
      def usage(str=nil)
        puts <<-EOS
Usage: eqm [options] [grep_string [another_grep_string [...]]]

This program parses the XML output of the "qstat -x" command and provides
a human-readable job state for all or a subset of the running jobs.

  grep_string -- a list of regular expressions can be provided
      to filter the output.  All must match for a line to be printed

Options:

  --user USERNAME or -u USERNAME -- display that user's jobs
      Default: display your ($USER) jobs.
  --all-users or -a -- display all users' jobs

  --key ELEMENT or -k ELEMENT -- Instead of printing the job name, print
      the "ELEMENT" XML element's text.  Several may be listed by giving
      the -k option multiple times (-k out -k err).  Some possibilities:

      -k out -- print the job stdout location
      -k err -- print the job stderr location
      -k exeguess -- print a guess as to the executable path
      -k submit_host -- host that submitted the job
      -k Mail_Points -- mailing options
      -k queue -- queue name
      -k walltime -- wall clock used
      -k nodes -- nodes/procs resource specification

      All are case-sensitive.

  --queue-manager NAME -- force eqm to use a specific queue management system,
      instead of guessing based on what cluster you're on.  Valid values:

         torque -- Torque queue manager (qstat -x)
         gridengine -- Sun/Oracle Grid Engine (qstat)
         moab -- moab queue manager (showq --xml)
         lsf -- IBM LSF queue manager (bjobs -l)
         loadleveler -- IBM LoadLeveler (llq -l)

  --grep regex or -g regex -- discard any lines that don't match this regular
      expression (case-insensitive).
  --antigrep regex or -v regex -- discard lines that DO match this regular
      expression (case-insensitive).

  --no-colors or -n -- disable colors
  --clear-screen or -c -- clear screen and loop, displaying the list multiple 
      times and sleeping in between
  --loop N or -l N -- same as -c, but does not clear the screen
  --sleep-time N or -s N -- how long to sleep between checks
  --loops N or -M N -- number of times to loop

  --cache-location /path/to/file or -L /path/to/file -- cache results in
      this location to be used by later eqm commands
  --max-age N or -m N -- maximum age of cache file before it is regenerated
  --qstat-path /path/to/qstat -- path to the qstat command
  --disable-caching or -C -- disable caching support
  --force-caching or -N -- use the cache file no matter how old it is
EOS
        if(str!=nil) then
          abort "Exiting due to invalid arguments: #{str}"
        else
          exit
        end
      end
      def usage_int(opt,arg)
        begin
          return Integer(arg)
        rescue
          usage("Unable to convert \"#{arg}\" to an integer in #{opt} option.")
        end
      end

      def parse_dash_options(argv)
        # Parse argument list:
        req=GetoptLong::REQUIRED_ARGUMENT
        noarg=GetoptLong::NO_ARGUMENT
        opts = GetoptLong.new(["--llq-path", "-Q", req],
                              ["--remove-done", "-d", noarg],
                              ["--bjobs-path", req],
                              ["--bhist-path", req],
                              ["--qstat-path", req],
                              ["--pbsquery-path", req],
                              ["--hhs-ens", req],
                              ["--showq-path", req],
                              ["--key", '-k', req],
                              ["--no-colors", '-n', noarg],
                              ["--repitition", "-R", req],
                              ["--cache-location", "-L", req],
                              ["--max-age", "-m", req],
                              ["--grep", "-g", req],
                              ["--antigrep", "-v", req],
                              ["--verbose",noarg],
                              ["--clear-screen", "-c", noarg],
                              ["--loop", "-l", noarg],
                              ["--loops","-M", req],
                              ["--user", "-u", req],
                              ["--all-users", "-a", noarg],
                              ["--sleep-time", "-s", req],
                              ["--manual-options", "-o", req],
                              ["--force-blocking", '-b', noarg],
                              ["--disable-caching", '-C', noarg],
                              ["--force-caching", '-N', noarg],
                              ["--auto-update", '-U', noarg],
                              ["--emulation-mode", '-e', req],
                              ["--no-auto-update", noarg],
                              ["--help",'-h',noarg],
                              ["--checkjob-path",req],
                              ["--queue-manager",req],
                              ["--running-zombie-age",req],
                              ['--bhist-options',req]
                             )

        opts.each do |opt,arg|
          case opt
          when '--bhist-options'    ; @bhist_options=arg;
          when '--running-zombie-age' ; @running_zombie_age=arg;
          when '--hhs-ens'          ; @hhs_ens=arg;
          when '--verbose'          ; @verbose=true
          when '--remove-done'      ; @no_complete=true
          when '--queue-manager'    ; @queue_manager=arg
          when '--help'             ; usage()
          when '--qstat-path'       ; @qstat_path=arg
          when '--pbsquery-path'    ; @pbsquery_path=arg
          when '--bjobs-path'       ; @bjobs_path=arg
          when '--bhist-path'       ; @bhist_path=arg
          when '--llq-path'         ; @llq_path=arg
          when '--showq-path'       ; @showq_path=arg
          when '--checkjob-path'    ; @checkjob_path=arg
          when '--no-colors'        ; @nc=true
          when '--cache-location'   ; @cache_file=arg
          when '--max-age'          ; @max_age=usage_int(opt,arg)
          when '--key'              ; @vars.push(arg)
          when '--grep'             ; add_regexp(arg,@greps)
          when '--antigrep'         ; add_regexp(arg,@antigreps)
          when '--clear-screen'     ; @clear_screen=true ; @looping=true
          when '--loop'             ; @looping=true
          when '--loops'            ; @loops=usage_int(opt,arg)
          when '--user'             ; @user=arg
          when '--all-users'        ; @user=nil
          when '--sleep-time'       ; @sleep_time=usage_int(opt,arg)
          when '--manual-options'
            @manual_options=arg
            @disable_caching=true
            @only_cache=false
          when '--force-blocking'   ; @force_blocking=true
          when '--auto-update'      ; @auto_update=true
          when '--no-auto-update'   ; @auto_update=false
          when '--llq-path'
            #    warn "Ignoring argument #{arg} since it is not relevant to Moab."
          when '--emulation-mode'
            @emu_mode=arg
          when '--force-caching'
            @only_cache=true
            @disable_caching=false
          when '--disable-caching'
            @only_cache=false
            @disable_caching=true
          when '--repitition'
            match=arg.match(/\A(\d+):(\d+)\z/)
            if(match) then
              @reps=usage_int('--repitition first arg',match[1])
              @rep_sleep=usage_int('--repitition second arg',match[2])
            else
              usage "Invalid #{opt} value \"#{arg}\"\n";
            end
          else
            usage("Unknown option #{opt}.")
          end
        end

        if(!@looping)
          @loops=1
        end

        if(@max_age>@sleep_time && !@disable_caching) then
          warn("Max age #{@max_age} is longer than sleep time #{@sleep_time} so some results will be repeated.")
        end
        if(@only_cache && @cache_file==nil) then
          usage 'When disabling running of qstat, you must specify a cache file.'
        end
      end

      def parse_other_args(argv)
        if(@emu_mode=='jobspecs')
          if(argv.length<3)
            usage("hwrf_hhs_jobspecs.pl mode requires at least three arguments.  Also, don't run in this mode unless you are HHS.")
          end
          @colormap=@hhs_colormap
        end

        if(@emu_mode=='jobspecs') then
          (@hhs_stid,@hhs_hwrfdata,@hhs_logdir,@hhs_jobid,@hhs_extra,@hhs_kick)=argv
          @hhs_stid=@hhs_stid.downcase
          argv.shift(6)
        end

        if(@emu_mode!=nil) then
          if(@user==nil) then
            if(@emu_printers["#{@emu_mode}-all"]!=nil) then
              @printers=@emu_printers["#{@emu_mode}-all"]
              warn "using printers for #{@emu_mode}-all" if @verbose
            else
              @printers=@emu_printers[@emu_mode]
              warn "using printers for @emu_mode" if @verbose
            end
          else
            warn "using printers for @emu_mode" if @verbose
            @printers=@emu_printers[@emu_mode]
          end
          @colormap=@emu_colormaps[@emu_mode]
        else
          warn "no emulation mode specified, using eqm" if @verbose
          @emu_mode='eqm'
          warn "using printers for @emu_mode" if @verbose
          @printers=@emu_printers[@emu_mode]
          @colormap=@emu_colormaps[@emu_mode]
        end

        # @emu_printers.each { |k,v|
        #   puts "have mode for (#{k})" if(v!=nil)
        # }

        # raise "no qerr mode" if @emu_printers['qerr']==nil
        # raise "no emulation mode #{@emu_mode}" if @printers==nil

        @colormap=@emu_colormaps['default'] if(@colormap==nil)
        if(@user==nil) then
          warn "using printers for default-all" if @verbose
          @printers=@emu_printers['default-all'] if(@printers==nil)
        else
          warn "using printers for default" if @verbose
          @printers=@emu_printers['default'] if(@printers==nil)
        end

        return if @emu_mode=='qhist'
        
        while(argv.length>0)
          #puts "argv[0]=\"#{argv[0]}\" and argv[0][0..0]=\"#{argv[0][0..0]}\"."
          if(argv[0] == '+nohead') then
            @nohead=true
          elsif(argv[0] == '+nofoot') then
            @nofoot=true
          elsif(argv[0] == '+clear') then
            @colormap={}
            @printers={}
          elsif(argv[0][0..6]=='+sort:=') then
            @sorting=argv[0]
            if(@sorting.length>7)
              @sorting=@sorting[7..@sorting.length];
            else
              @sorting=''
            end
            @sort_order=[]
            @sorting.scan(/([ns])([+-])([a-z0-9A-Z_\/]+)/).each do |match|
              if(match[0]!=nil && match[1]!=nil && match[2]!=nil) then
                @sort_order.push([ String(match[0]), Integer(match[1]+'1'), String(match[2]) ])
              end
            end
          elsif(argv[0][0..0]=='+') then
            colorize=argv[0].scan(/\A\+([A-Za-z]+):([a-zA-Z0-9_+-]*)\z/)[0]
            if(colorize!=nil && colorize.length>0) then
              (key,color)=colorize
              if(color!=nil && color!='') then
                @colormap[key]=color
              else
                @colormap.delete(key)
              end
            else
              matches=argv[0].scan(/\A\+([A-Za-z]+):([a-zA-Z0-9_+-]*)=(.*)\z/)[0]
              if(matches!=nil && matches.length==3) then
                (key,color,value)=matches
                if(color!=nil && color!='') then
                  @colormap[key]=color
                end
                if(value!=nil && value!='') then
                  gotformat=true
                  @printers[key]=value
                end
              else
                warn "Cannot understand \"#{argv[0]}\""
              end
            end
          else
            #warn "Assuming this is a grep string: \"#{argv[0]}\""
            add_regexp(argv[0],@greps)
          end
          argv.shift()
        end
        # @printers.each { |k,v|
        #  puts "printer (#{k}) => (#{v})"
        #}
        # @colormap.each { |k,v|
        #  puts "colormap (#{k}) => (#{v})"
        #}
      end

      def color_for(state)
        if(@colormap==nil)
          return 'none'
        end
        color=@colormap[state]
        if(color==nil)
          @groups.each{ |group|
            if(@colormap[group]!=nil) then
              group.each{ |item|
                if(item==state) then
                  return @colormap[group]
                end
              }
            end
          }
        end
        if(color==nil)
          color=@colormap['default']
        end
        if(color==nil)
          color='none'
        end
      end
    end
  end
end
