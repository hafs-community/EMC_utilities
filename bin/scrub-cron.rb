#! /usr/bin/env ruby

# Scrubs old cron jobs.  Reads the user's crontab and prints a scrubbed version to stdout.  
#
# Looks for blocks like this:
#     #:# until 2018091418
#     10 10 * * * /bin/true
#     #:# end until
#
#     #:# until 2003091419
#     10 10 * * * /bin/echo > /dev/null
#     #:# end
#
# Removes a block of cron jobs once its "until" date has been reached.
# Any cron jobs not in an "until" block are passed through unmodified.
#
# On error, prints the original crontab to stdout (if possible) and
# exits with non-zero status.  Note that the stdout could be empty if
# "crontab -l" failed or if the original crontab was empty.

require "date"

oldtab=`crontab -l`
newtab=[]

start=DateTime.now
lastdate=nil
block=[]
iline=0
until_start_line=0
error=false

oldtab.each_line do |line|
  iline=iline+1
  if line=~/^#:#/
    if line=~/^#:#\s*end(\s+until)?\s*$/
      if lastdate.nil?
        STDERR.puts("crontab:#{iline.to_s}: \"end\" without \"until\"")
        error=true
      elsif lastdate>=start
        block << line
        newtab.concat(block)
      end
      block=[]
      lastdate=nil
    elsif line=~/^#:#\s+until\s+(\d\d\d\d)(\d\d)(\d\d)(\d\d)\s*$/
      if lastdate.nil?
        lastdate=DateTime.new($1.to_i,$2.to_i,$3.to_i,$4.to_i)
        block << line
        until_start_line=iline
      else
        STDERR.puts("crontab:#{iline.to_s}: \"until\" block inside \"until\" block")
        error=true
      end
    else
      STDERR.puts("crontab:#{iline.to_s}: invalid #:# line.  Must be \"#:# end until\" or \"#:# until YYYYMMDDHH\"")
      error=true
    end
  elsif lastdate.nil?
    newtab << line
  else
    block << line
  end
end

if not block.empty?
  STDERR.puts("crontab:#{until_start_line.to_s}: unterminated #:# until block")
  newtab.concat(block)
end

if error
  puts oldtab
  exit(1)
else
  puts newtab.join("")
end
