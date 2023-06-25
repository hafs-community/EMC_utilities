require 'emc/qmonitor'
require 'emc/asnum'

require 'rexml/namespace'
require 'rexml/document'
require "getoptlong"
require 'fileutils'
require 'time'
require 'pathname'

module EMC
  module Queues
    ########################################################################
    ## CLASS TerminalColormap ##############################################
    ########################################################################

    class TerminalColormap
      def initialize(colormap,groupmap,nocolor)
        @groupmap=groupmap
        @colormap=colormap
        @nocolor=nocolor
      end

      # Subroutine color_off
      #   Returns the string that switches the terminal back to default
      #   coloring
      #   If disable is true, then '' is returned
      def color_off(disable)
        if(disable) then
          return ''
        else
          return "\e[0m"
        end
      end

      # Subroutine term_color
      #   Parses a list of color and other display attribute information and
      #   returns a VT100 control code to set that terminal status.  The
      #   special value of nil is returned when the color is "noprint"
      #
      #   color -- return value from job_color
      #   disable -- if true, coloring is disabled
      def term_color(color,disable)
        if(color=='noprint') then
          return nil
        end
        if(disable) then
          return ''
        end
        c=color
        if(c==nil)
          c='none'
        end
        fgs={ 'black'=>30, 'red'=>31, 'green'=>32, 'yellow'=>33,
          'blue'=>34, 'magenta'=>35, 'purple'=>35, 'cyan'=>36, 'white'=>37}
        bgs={ 'bgblack'=>40, 'bgred'=>41, 'bggreen'=>42, 'bgyellow'=>43,
          'bgblue'=>44, 'bgmagenta'=>45, 'bgpurple'=>45, 'bgcyan'=>46, 'bgwhite'=>47 }
        attrmap={ 'bold'=>1, 'dim'=>2, 'underline'=>4, 'blink'=>5, 'reverse'=>7,
          'hidden'=>8 }

        out=[0] # out will contain the final color command list
        bg=nil  # final foreground color
        fg=nil  # final background color
        # Attribute enable/disable flags:
        attr={ 'bold'=>false, 'underline'=>false, 'reverse'=>false,
          'hidden'=>false, 'dim'=>false, 'blink'=>false }

        # Loop over all attribute specifiers:
        c.split(/\+/).each do |m|
          # Attribute specifier (which may be a string or number) is in "m"

          if(m=='noprint') then
            return nil
          end

          # If m begins with "no" (ie.: noblink) then nono_m is the same,
          # but without the "no":
          nono_m=m
          nono_m=nono_m.gsub(/\Ano/,'')

          # Something like "no6" will break that, so no negating numbers.

          # Numeric ms are added verbatim:
          if(m =~ /\A\d+\z/) then
            out.push(Integer(m))
          else
            # Non-numeric ms search the fg, bg and attribute list:
            if(fgs[m]!=nil) then
              fg=fgs[m]
            elsif(bgs[m]!=nil) then
              bg=bgs[m]
            elsif(attrmap[m]!=nil) then
              attr[m]=true
            elsif(attrmap[nono_m]!=nil) then
              attr[m]=false
            end
          end
        end

        # Add attributes, bg and fg color to out:
        attr.each do |key,value|
          if(value) then
            out.push(attrmap[key])
          end
        end
        if(bg!=nil) then
          out.push(bg)
        end
        if(fg!=nil) then
          out.push(fg)
        end

        # Print the VT100 color code:
        return "\e["+out.join(";")+"m";
      end

      def color_for(what)
        if(what==nil || what=~/\A\s*\z/) then
          color=@colormap['default']
        else
          color=@colormap[what]
          if(color==nil) then
            @groupmap.each { |gname,list|
              gcolor=@colormap[gname]
              next if gcolor==nil
              list.each { |gwhat|
                if(gwhat==what) then
                  color=gcolor
                  break
                end
              }
              break if color!=nil
            }
          end
        end
        if(color==nil) then
          color=@colormap['default']
        end
        if(color==nil) then
          color='none'
        end
        
        return color
      end
      
      # job_color: determines the color string that should be sent to
      #   term_color for the specified job.  If the color is not defined
      #   then "none" is returned.
      def job_color(job)
        return color_for(job['state'])
      end
    end

    class EQMOptions
      def get_colormap()
        tempcolormap=@colormap
        if(@no_complete)
          tempcolormap=tempcolormap.clone
          tempcolormap['done']='noprint'
        end
        return TerminalColormap.new(tempcolormap,@groups,@nc)
      end
    end

    ########################################################################

    class JobPrinter
      def initialize(printmap,groupmap)
        @printmap=printmap
        @groupmap=groupmap

        footers={}
        headers={}

        @printmap.each { |k,v|
          #puts "#{k}=>#{v}"
          if(k=~/\Afoot(\d+)\z/) then
            #puts "... is a footer"
            # This is a footer
            footers[$1]=v
          elsif(k=~/\Ahead(\d+)\z/) then
            #puts "... is a header"
            # This is a header
            headers[$1]=v
          end
        }

        @footers=[] ; @headers=[]

        footers.keys.sort{ |x,y| x.to_i <=> y.to_i }.each {|k|
          @footers.push(footers[k])
        }
        headers.keys.sort{ |x,y| x.to_i <=> y.to_i }.each {|k|
          @headers.push(headers[k])
        }
      end

      def each_footer
        @footers.each { |f| yield f }
      end
      def each_header()
        @headers.each { |h| yield h }
      end

      def get_printer(job)
        state=job['state']

        printer=nil # return nil when no printer is specified

        if(state==nil || state=~/\A\s*\z/) then
          printer=@printmap['default']
        else
          printer=@printmap[state]
          if(printer==nil) then
            @groupmap.each { |gname,list|
              gprinter=@printmap[gname]
              next if gprinter==nil
              list.each { |gstate|
                if(gstate==state) then
                  printer=gprinter
                  break
                end
              }
              break if printer!=nil
            }
          end
        end
        if(printer==nil) then
          printer=@printmap['default']
        end

        return printer
      end
    end

    class EQMOptions
      def get_printmap()
        jp=JobPrinter.new(@printers,@groups)
        return jp
      end
    end

    ########################################################################
    ## CLASS QueueReporter #################################################
    ########################################################################

    # This class handles display of information from a QueueState object.

    class QueueReporter
      include EMC::AsNum
      def initialize(state,options)
        @state=state
        @opts=options
        @sorted=nil
      end
      def color_disabled()
        return @opts.color_disabled
      end
      def update()
        #puts "call state.update"
        # @state.update()
        # Sort jobs first by name (reversed) then by ID
        return @sorted
      end
      def run()
        nohead=@opts.nohead
        nofoot=@opts.nofoot
        sorder=@opts.sort_order
        colors=@opts.get_colormap
        printers=@opts.get_printmap
        clear=@opts.clear_screen
        loops=@opts.loops
        naptime=@opts.sleep_time
        nc=@opts.color_disabled()
        greps=@opts.greps
        antigreps=@opts.antigreps

        loops=1 if loops<1

        for iloop in 1..loops
          if(clear && iloop==1) then
            puts "Thinking..."
          end
          
          # Update the queue state:
          update()
          
          # Clear the screen if we are told to do so
          if(clear && !nc)
            print "\e[0m\e[H\e[J"
          end
          
          # Show the queue state:
          show_headers(colors,printers) unless nohead
          show(sorder,colors,printers,greps,antigreps)
          show_footers(colors,printers) unless nofoot

          # Sleep if needed:
          if(iloop<loops) then
            sleep(naptime)
          end
        end
      end
      def job_for(jobid)
        return @state[jobid]
      end
      def sorted(sorting)
        # This returns the job IDs in sorted order, based on the sorting
        # specified in the argument.
        jobkeys=@state.jobids
        if(sorting==nil) then
          raise 'sorting is nil'
          # special case: nil sorting means "return hash ordering"
          return jobkeys
        end
        sorted=jobkeys.sort{ |x,y|
          jobx=job_for(x)
          joby=job_for(y)
          order=0
          @opts.sort_order.each do |sorting|
            #puts "#{x} vs #{y}: #{sorting} comparison."
            xvar=jobx[sorting[2]]
            yvar=joby[sorting[2]]
            #puts "#{xvar} <=#{sorting[1]}#{sorting[0]}=> #{yvar}"
            if(sorting[0]=='s') then
              # string compare
              order=sorting[1] * (xvar <=> yvar)
            else
              # numeric compare
              order=sorting[1] * (asnum(xvar) <=> asnum(yvar))
            end
            #puts "order = #{order}"
            if(order!=0) 
              break
            end
          end
          #puts "final order = #{order}"
          order
        }
        return sorted
      end
      def show_headers(colormap,printmap)
        show_head_foot(colormap,printmap,true)
      end
      def show_footers(colormap,printmap)
        show_head_foot(colormap,printmap,false)
      end
      def show_head_foot(colormap,printmap,head)
        now=Time.new()
        morevars={
          'Q_AGE'=>(now-@state.queue_time).to_i,
          'Q_AGE_TYPE'=>@state.queue_age_type,
          'Q_SOURCE'=>@state.queue_from
        }
        if(head) then
          color=colormap.color_for('head')
        else
          color=colormap.color_for('foot')
        end
        return if color==nil || color=='noprint'

        nc=color_disabled()

        prefix=''
        if(colormap!=nil) then
          suffix=colormap.color_off(nc)
        else
          suffix=''
        end
        if(colormap!=nil) then
          prefix=colormap.term_color(color,nc)
        end

        dummyjob=@state[nil].morevars(morevars)
        if(head) then
          printmap.each_header() { |hprinter|
            puts prefix+dummyjob.expand(hprinter).to_s+suffix
          }
        else
          printmap.each_footer() { |fprinter|
            puts prefix+dummyjob.expand(fprinter).to_s+suffix
          }
        end
      end
      def show(sort_order,colormap,printmap,greps,antigreps)
        # This function displays the queue state once, using the current
        # queue state (without calling update), based on the current
        # options.  No checks are done as to whether it is time to show
        # the queue state.  It is assumed that the caller will take care
        # of that.  Also, no headers or footers are shown.  

        now=Time.new()
        morevars={
          'Q_AGE'=>(now-@state.queue_time).to_i,
          'Q_AGE_TYPE'=>@state.queue_age_type,
          'Q_SOURCE'=>@state.queue_from
        }

        nc=color_disabled()
        if(colormap!=nil) then
          suffix=colormap.color_off(nc)
        else
          suffix=''
        end

        sorted(sort_order).each { |jobid|
          catch :skip_line do
            job=job_for(jobid)
            prefix=''
            color='none'

            printer=printmap.get_printer(job)
            next if(printer==nil)

            if(colormap!=nil) then
              color=colormap.job_color(job)
            end
            next if(color==nil || color=='noprint')

            if(colormap!=nil) then
              prefix=colormap.term_color(color,nc)
            end

            expanded=job.morevars(morevars).expand(printer).to_s

            greps.each { |regex|
              if( ! (expanded =~ regex) ) then
                throw :skip_line
              end
            }
            
            antigreps.each { |regex|
              if( expanded =~ regex ) then
                throw :skip_line
              end
            }
            
            puts prefix+expanded+suffix
          end
        }
      end
    end
  end
end
