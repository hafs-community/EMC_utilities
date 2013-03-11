#! /bin/ksh --login
#PBS -l procs=3
#PBS -o /lfs1/projects/hwrf-vd/emc-utils/src/mpiserial.cd/zeus-test.log
#PBS -joe
#PBS -l partition=njet:tjet:ujet:sjet
#PBS -A hwrfv3
#PBS -d /lfs1/projects/hwrf-vd/emc-utils/src/mpiserial.cd
#PBS -l walltime=00:10:00
#PBS -m n
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

if [[ -s /apps/Modules/3.2.9/init/ksh ]] ; then
    . /apps/Modules/3.2.9/init/ksh
fi
module load intel
module load hsms
module load mvapich2

mpiserial=/lfs1/projects/hwrf-vd/emc-utils/src/mpiserial.cd/mpiserial

echo ====== /bin/true
mpiexec $mpiserial /bin/true
echo status $?

echo ====== /bin/false
mpiexec $mpiserial /bin/false
echo status $?

echo ====== one rank false, others sleep and echo
cat<<EOF > cmdfile
/bin/false
/bin/sh -c 'echo start ; sleep 3 ; echo done ; exit 0'
/bin/sh -c 'echo start ; sleep 10 ; echo done ; exit 0'
EOF
mpiexec $mpiserial
echo status $?

echo ===== get rank and comm size
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec $mpiserial
echo status $?

echo ===== too many ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 3 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec $mpiserial
echo status $?

echo ===== too few ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec $mpiserial
echo status $?
