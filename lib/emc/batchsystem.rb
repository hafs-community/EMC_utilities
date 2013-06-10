#! /usr/bin/env ruby

require 'etc'
begin
  require 'yaml'
rescue LoadError
end
require 'socket'

require 'emc/jobstep'

class String
  @@today=nil
  def parseOptions()
    # Is the string a sequence of name=value pairs?
    if(self=~/\A

\s* ([a-zA-Z0-9_-]+) \s* 
(?: [:=] \s* 
 (?:
     \"((?:[^\"\\]*|\\\\|\\.)*)\"
   | ((?:[^,\"\\]|\\\\|\\.)*)
 )
)?
(?:
  \s*,\s*
  ([a-zA-Z0-9_-]+) \s*
  (?: [:=] \s* 
     (?:
         \"((?:[^\"\\]*|\\\\|\\.)*)\"
        | ((?:[^,\"\\]|\\\\|\\.)*)
     )
  )?
)  *
\s*\z
/x)
      # Rejoice.
    else
      # Invalid string.
      fail "Invalid key=value list #{self}"
    end

    # Parse each pair.
    matches=self.scan(/\G \s*
([a-zA-Z0-9_-]+) \s* 
(?: [:=] \s* 
  (?:
       \"((?:[^\"\\]*|\\\\|\\.)*)\"
    |  ((?:[^,\"\\]|\\\\|\\.)*)
  )
)?
(?:\s*,|\s*$)/x)

    matches.each { |matches|
      key=matches[0]
      value=matches[1]
      value=matches[2] if value.nil?
      value=value.gsub(/\\(.)/,'\1') unless value.nil?
      yield key,value
    }
  end

  def to_timespan()
    if(self=~/^0*(\d+):0*(\d+)$/)
      return ($1.to_i*60+$2.to_i)*60.to_i
    elsif(self=~/^0*(\d+):0*(\d+):0*(\d+)$/)
      return ($1.to_i*60+$2.to_i)*60+$3.to_i
    elsif(self=~/^(\d+)$/)
      return $1.to_i*60
    else
      fail "Unrecognized timespan #{self}"
    end
  end

  def to_megabytes()
    if(self=~/^0*(\d+)\s*([tgmk]?)b?$/i)
      size,unit=$1.to_i,$2.downcase
      size/=1048576.0  if unit==''
      size/=1024.0     if unit=='k'
      size*=1024.0     if unit=='g'
      size*=1048576.0  if unit=='t'
      return size.ceil
    elsif(self=~/^0*(\d+)$/)
      return $1.to_i
    end
  end

  def to_epoch_time(now=nil,today=nil,tomorrow=nil)
    tzstore=ENV['TZ']
    ENV['TZ']='UTC'

    if(@@today.nil?)
      tnow=DateTime.now
      nowday=tnow.to_date
      @@today=DateTime.parse(nowday.to_s).to_time
      @@tomorrow=DateTime.parse((nowday+1).to_s).to_time
      @@now=tnow.to_time
    end
    time=nil

    now=@@now if now.nil?

    if(self=~/^(\d\d\d\d)(\d\d)(\d\d)(\d\d)(\d\d)?$/)
      # Run at specified date yyyymmddhh(mm)
      time=DateTime.new($1,$2,$3,$4,
                         ($5.nil?) ? 0 : $5).to_time.to_i
    elsif(self=~/^\+(\d\d)(\d\d)?$/)
      time=now + $1.to_i*3600
      time+=$2.to_i*60 unless($2.nil?)
      time=time.to_i
    elsif(self=~/^\+(\d+)m(?:in)?$/i)
      time=(now + $1.to_i*60).to_i
    elsif(self=~/^\+(\d+)h(?:r|our|rs|ours)?$/i)
      time=(now + $1.to_i*3600).to_i
    elsif(self=~/^t(\d\d)(\d\d)?$/)
      today=@@today if(today.nil?)
      time=today + 3600*$1.to_i
      time+=$2.to_i*60 unless($2.nil?)
      time=time.to_i
    elsif(self=~/^T(\d\d)(\d\d)?$/)
      tomorrow=@@tomorrow if(tomorrow.nil?)
      time=tomorrow + 3600*$1.to_i
      time+=$2.to_i*60 unless($2.nil?)
      time=time.to_i
    else
      fail "I do not understand date/time string \"#{self}\"."
    end

    ENV['TZ']=tzstore
    return time
  end
end


class BatchSys
  @@calls=0
  @@localBatchSysGuess=nil
  @@localQManagerGuess=nil
  attr_reader :where_am_i
  attr_accessor :remotecmd
  def initialize(where_am_i,remotecmd=nil)
    @where_am_i=where_am_i
    @remotecmd=remotecmd
#    fail "nil remotecmd" if @remotecmd.nil?
#    warn "remotecmd=\"#{remotecmd}\" in "+caller().join("\n")+"\n"
    #warn "I AM HERE: "+@where_am_i.join(',')
  end
  def whereAmI()
    return where_am_i
  end
  def myBatchSysName()
    fail "This BatchSys subclass has not defined myBatchSysName"
  end

  def jobSetup(job,justPrint)
    # Run this function to prepare to run the job.
    # By default, we have nothing to do.
  end

  def self.guessLocalBatchSys
    # Try to guess what cluster we're on
    if(!@@localBatchSysGuess.nil? && !@@localQManagerGuess.nil?)
      return @@localBatchSysGuess,@@localQManagerGuess
    else
      cluster,qmanager=nil,nil
    end
    if(File.exist?('/pan2') || File.exist?('/lfs2'))
      ENV['PATH'].split(':').each { |path|
        if(path=~/torque/ && File.executable?("#{path}/qsub"))
          # We're on the new jet
          cluster,manager = 'ujet','Torque'
        end
      }
      if(cluster.nil?)
        cluster,qmanager = 'tjet','GridEngine'
      end
    elsif(File.exist?('/ttfri'))
      cluster,qmanager = 'ttfrisaola','Torque'
    elsif(File.exist?('/scratch1') || File.exist?('/scratch2'))
      cluster,qmanager = 'zeus','Torque'
    elsif(File.exist?('/lustre/fs') || File.exist?('/lustre/ltfs'))
      cluster,qmanager = 'gaea','Moab'
    elsif(File.exist?('/com'))
      # On an NCEP machine
      hostname=Socket.gethostname()
      case hostname[0,1]
      when 'c','C'
        if(File.exist?('/dev/kmsg'))
          cluster,qmanager = 'current','none'
        else
          cluster,qmanger = 'cirrus','LoadLeveler'
        end
      when 's','S'
        cluster,qmanager = 'stratus','LoadLeveler'
      when 'e','E'
        cluster,qmanager = 'eddy','LSF'
      when 't','T'
        cluster,qmanager = 'tide','LSF'
      when 'g','G'
        cluster,qmanager = 'gyre','LSF'
      else
        cluster,qmanager = 'unknownNCEP','unknown'
      end
    end
    cluster='unknown' if cluster.nil?
    qmanager='unknown' if qmanager.nil?
    @@localBatchSysGuess=cluster
    @@localQManagerGuess=qmanager
    # warn "BatchSys is #{cluster}"
    return cluster,qmanager
  end

  def nonLocalJob?(host,user)
    if(host.nil? || host==myBatchSysName())
      # Local machine, so this is only a "non-local" job if the user is different:
      return Etc.getlogin==user
    else
      # Not the local machine, so definitely not a local job
      return true
    end
  end

  def self.localBatchSys()
    cluster,qmanager=BatchSys.guessLocalBatchSys()
    if(cluster=='unknown' || cluster=='unknownNCEP')
      fail "Cannot guess local cluster (#{cluster})"
    else
      return BatchSys.forHost(cluster,nil,false)
    end
  end

  def remoteBatchSys(clusterName=nil,userName=nil)
    return remoteBatchSysImpl(clusterName,userName)
  end

  def requestRemoteSubmission(remoteBatchSys,remoteUser,localUser)
    # This routine's purpose is to call "fail" if the specified remote
    # job submission route is impossible.  The "remoteUser" is nil if
    # it was not specified.
    return true
  end

  def requestLocalSubmission(user)
    # This routine's purpose is to call "fail" if the current user
    # cannot submit jobs as the specified user (who is guranteed to be
    # different).  If the user was unspecified, it will be nil.
    return true
  end

  def remoteBatchSysImpl(clusterName=nil,userName=nil)
    me=Etc.getlogin
    #warn "cluster remoteBatchSys"

    if(nonLocalJob?(clusterName,userName))
      # Remote cluster OR local cluster but with a different username
      if(remote)
        requestRemoteSubmission(clusterName,userName,me)
      else
        requestLocalSubmission(userName)
      end
      fh=BatchSys.forHost(clusterName,userName,true)
      if(fh.nil?)
        fail "forHost returned nil"
      end
      return fh
    else
      # Local job as my user OR no remote info specified.
      if(!userName.nil?)
        requestLocalSubmission(userName)
      end
      return self
    end
  end
  def makeJobStepImpl(io,wantBatchSys=false)
    js=JobStep.new(@where_am_i)
    
    # First stage of parser: figure out the remote host.
    # We do this stage as the local machine so that, for example
    #   @tide: -R gyre
    # lines will be run if the local host is Tide
    js.findRemoteHost(io)
    
    # Second stage parser: parse as remote machine, ignore -R lines:
    js.parse(io)

    return js
  end
  def makeJobStep(io)
    js=makeJobStepImpl(io,false)
    return js
  end
  def launchJob(io,justPrint=false,printStream=STDOUT)
    js=makeJobStepImpl(io)

    remoteHost=js.remoteHost
    fail "Invalid remote host" if !remoteHost.nil? && remoteHost==''

    if(nonLocalJob?(js.remoteHost,js.remoteUser))
      cluster=BatchSys.forHost(js.remoteHost,js.remoteUser,true,myBatchSysName()) 
      if(cluster.nil?)
        fail "Internal error: BatchSys.forHost returned nil for #{js.remoteUser} on #{js.remoteHost}"
      end
    else
      cluster=self
    end
    cluster.launchJobImpl(io,justPrint,printStream)
  end
  def launchJobImpl(jobStep,justPrint=false,printStream=STDOUT)
    fail "You must use a BatchSys subclass, not BatchSys itself."
  end
  def jobCard(jobStep)
    fail "You must use a BatchSys subclass, not BatchSys itself."
  end
  def timezone()
    fail "You must use a BatchSys subclass, not BatchSys itself."
  end
  def convertPath(path)
    return path
  end
end

# Load all local batch system implementations:

require 'emc/batchsystem/zeus'
require 'emc/batchsystem/jet'
require 'emc/batchsystem/ttfri'
require 'emc/batchsystem/ccs'
require 'emc/batchsystem/wcoss'

# Now we can define forHost, which creates the correct BatchSys
# subclass for a specific machine:

class BatchSys
  def self.forHost(host,user,allowRemote,myBatchSysName='unknown')
    @@calls+=1
    if(@@calls>10)
      fail "infinite recursion"
    end
    strhost=host.to_s.downcase
    me=myBatchSysName
    user_at=''
    user_at="#{user}@" unless user.nil?

    if(host.nil?)
      fail "Argument \"host\" must not be nil in BatchSys.forHost"
    end

    if(!user.nil? && strhost=~/^.jet/)
      fail "Cannot submit jobs as a specified user on any of the Jets.  You must rerun without specifying the user in your job card."
    end
    if(!user.nil? && strhost=='zeus')
      fail "Cannot submit jobs as a specified user on Zeus."
    end
    if(!user.nil? && strhost=='.*erin.*')
      fail "Cannot submit jobs as a specified user on TTFRI HP (Erin)."
    end

    case strhost
    when 'cirrus','stratus'
      if(allowRemote && me!=strhost)
        return CCSBatchSys.new([strhost],"ssh #{user_at}#{strhost}")
      else
        return CCSBatchSys.new([strhost])
      end
    when 'tide','gyre'
      if(allowRemote && me!=strhost)
        return WCOSSBatchSys.new([strhost],"ssh #{user_at}#{strhost}") 
      else
        return WCOSSBatchSys.new([strhost])
      end
    when 'ttfrihp'
      return TTFRIHPBatchSys.new()
    when 'ttfri','ttfrisaola'
      return TTFRISaolaBatchSys.new()
    when 'sjet'
      return SJetBatchSys.new()
    when 'tjet'
      return TJetBatchSys.new()
    when 'njet'
      return NJetBatchSys.new()
    when 'ujet'
      return UJetBatchSys.new()
    when 'zeus'
      return ZeusBatchSys.new()
    when 'gaea'
      fail "No GAEA support yet."
    else
      fail "I do not recognize host \"#{strhost}\""
    end
  end


end
