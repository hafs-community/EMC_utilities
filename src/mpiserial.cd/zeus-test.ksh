#! /bin/ksh --login
#PBS -l procs=3
#PBS -o /ptmp/Samuel.Trahan/zeus-test.log
#PBS -joe
#PBS -q batch
#PBS -A hwrf
#PBS -d /ptmp/Samuel.Trahan/
#PBS -l walltime=00:10:00
#PBS -m n
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1

if [[ -s /usr/share/Modules/init/ksh ]] ; then
    . /usr/share/Modules/init/ksh
fi
module load intel
module load hpss
module load adaptive
module load mpt

mpiserial=/contrib/emc-utils/versions/zeus-is-broken/src/mpiserial.cd/mpiserial

echo ====== /bin/true
mpiexec_mpt $mpiserial /bin/true
echo status $?

echo ====== /bin/false
mpiexec_mpt $mpiserial /bin/false
echo status $?

echo ====== one rank false, others sleep and echo
cat<<EOF > cmdfile
/bin/false
/bin/sh -c 'echo start ; sleep 3 ; echo done ; exit 0'
/bin/sh -c 'echo start ; sleep 10 ; echo done ; exit 0'
EOF
mpiexec_mpt $mpiserial
echo status $?

echo ===== get rank and comm size
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec_mpt $mpiserial
echo status $?

echo ===== too many ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 3 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec_mpt $mpiserial
echo status $?

echo ===== too few ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpiexec_mpt $mpiserial
echo status $?
