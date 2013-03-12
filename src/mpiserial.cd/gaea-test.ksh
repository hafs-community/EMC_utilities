#! /bin/ksh --login
#PBS -r y                                                              #This job is restartable
#PBS -j oe                                                             #STDOUT and STDERR are in the same file, specified below in the '-o' option
#PBS -l partition=c1                                                   #Do not change this - it tells your job where to run
#PBS -q batch                                                          #Do not change this - it tells your job where to run
#PBS -S /bin/sh                                                        #Do not change this - it keeps your job from issuing a false alarm
#PBS -E                                                                #Do not change this - it gives your job more and more useful Moab environment variables
#PBS -N gaea_test                                                      #Job Name:             You may want to change this to reflect your job's purpose or relation to an experiment
#PBS -d /lustre/fs/scratch/ncep/Samuel.Trahan/                                #Working directory: You MUST change this to a directory on /lustre/fs that exists and you have write permission on
#PBS -o /lustre/fs/scratch/ncep/Samuel.Trahan/${MOAB_JOBNAME}_${MOAB_JOBID}   #STDOUT directory:  You MUST change this to a directory on /lustre/fs that exists and you have write permission on
#PBS -l size=24                                                        #You will need to change this to reflect your job's core requirements - 24 is the current core increment.
#PBS -l walltime=00:10:00                                              #You MUST change this to reflect your job's expected run time, including copy time
#PBS -m bea                                                            #Email options: You may want to change this to reduce your incoming email stream
# Job card is copied from the c1ms skeleton job
. /etc/profile
export OMP_NUM_THREADS=1
module load PrgEnv-pgi/4.0.46
module load ibmpe
export MP_TASK_AFFINITY=core
export MP_EUIDEVICE=sn_all
export MP_EUILIB=us

mpiserial=/lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/bin/mpiserial

echo ====== /lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/src/mpiserial.cd/true
aprun -n 3 $mpiserial /lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/src/mpiserial.cd/true
echo status $?

echo ====== /lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/src/mpiserial.cd/false
aprun -n 3 $mpiserial /lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/src/mpiserial.cd/false
echo status $?

echo ====== one rank false, others sleep and echo
cat<<EOF > cmdfile
/lustre/fs/scratch/ncep/Samuel.Trahan/emc-utils/src/mpiserial.cd/false
/bin/sh -c 'echo start ; sleep 3 ; echo done ; exit 0'
/bin/sh -c 'echo start ; sleep 10 ; echo done ; exit 0'
EOF
aprun -n 3 $mpiserial
echo status $?

echo ===== get rank and comm size
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
aprun -n 3 $mpiserial
echo status $?

echo ===== too many ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 3 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
aprun -n 3 $mpiserial
echo status $?

echo ===== too few ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
aprun -n 3 $mpiserial
echo status $?
