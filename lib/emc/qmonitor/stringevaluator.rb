require "getoptlong"
require 'fileutils'
require 'time'
require 'pathname'

########################################################################
## CLASS StringEvaluator (and its helper classes) ######################
########################################################################

# This class and its helper classes implement the "functions" like
# .time, .shortpath, .fullpath, etc. in the eqm strings.  The
# StringEvalulator is a container that stores all known StringFunction
# objects.

module EMC
  module Queues

    class StringFunction
      def apply(str,fun,arg,job)
      end
    end

    class StringToTime < StringFunction
      def apply(str,fun,arg,job)
        begin
          tim=Time.at(Integer(str))
          if(arg!=nil && arg!='') then
            return tim.strftime(arg)
          else
            return tim.strftime("%m/%d %H:%M")
          end
        rescue
          if(arg!=nil && arg!='') then
            return arg.gsub(/%./,'??')
          else
            return "??/?? ??:??"
          end
        end
      end
    end

    class ShortPath < StringFunction
      @@root_links=nil
      def find_root_links()
        links=Hash.new()
        Dir.foreach('/') do |filename|
          fullfile="/"+filename
          if(File.symlink?(fullfile)) then
            links[fullfile]=File.readlink(fullfile)
          end
        end
        sorted=links.keys.sort{ |x,y| -(links[x].length <=> links[y].length) }
        out=[]
        sorted.each{ |key|
          out.push([links[key],key])
        }
        return out
      end
      def apply(str,fun,arg,job)
        if(@@root_links==nil) then
          @@root_links=find_root_links()
        end
        @@root_links.each{ |pair|
          shortstr=str.gsub(pair[0],pair[1])
          if(shortstr!=str && shortstr!=nil)
            return shortstr
          end
        }
        return str
      end
    end

    class FullPath < StringFunction
      #   @@homefinder=nil
      #   def findhome(user)
      #     if(@@homefinder==nil)
      #       @@homefinder=HomeFinder.new()
      #     end
      #     return nonil(@@homefinder[nonil(user)])
      #   end
      def apply(str,fun,arg,job)
        #     str=instr
        #     if(str=~/\$PBS_JOBNAME/) then
        #       str=str.gsub(/\$PBS_JOBNAME/,job['name'])
        #     end
        #     if(str=~/\A\$HOME/) then
        #       str=str.gsub(/\A\$HOME/,findhome(job['user']))
        #     end
        #     if(str=~/\A~[a-zA-Z0-9._-]+/) then
        
        #     elsif(str=~/\A~\z/ || str=~/\A~\//) then
        
        #     end
        if(! (str=~/\A[~\/]/)) then
          workdir=job['workdir']
          if(workdir!=nil && workdir!='') then
            there=job['workdir']+'/'+str
            return there.gsub(/\/+/,'/')
          end
        end
        return str
      end
    end

    class StringChangeCase
      def apply(str,fun,arg,job)
        case fun
        when 'lc','lower','tolower','downcase'
          return fun.downcase
        when 'uc','toupper','upper','upcase'
          return fun.upcase
        when 'capitalize','Capitalize','cap','Cap'
          return fun.capitalize
        else
          return str
        end
      end
    end

    class StringEvaluator
      @@default=nil
      def initialize()
        @funs={}
      end
      def add(name,fun)
        if(name!=nil) then
          @funs[name]=fun
        end
      end
      def self.get_default()
        if(@@default==nil) then
          d=StringEvaluator.new()
          d.add('fullpath',FullPath.new())
          d.add('shortpath',ShortPath.new())
          d.add('time',StringToTime.new())
          scc=StringChangeCase.new()
          d.add('lc',scc)
          d.add('uc',scc)
          d.add('cap',scc)
          @@default=d
        end
        return @@default
      end
      def evaluate(str,funname,arg,job)
        fun=@funs[funname]
        if(fun==nil) then
          return str
        else
          return fun.apply(str,funname,arg,job)
        end
      end
    end


  end
end
