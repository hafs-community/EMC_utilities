#! /bin/ksh --login
#BSUB -a poe
#BSUB -P HWRF-T2O
#BSUB -n 3
#BSUB -cwd .
#BSUB -oo tide-test.log
#BSUB -R span[ptile=3]
#BSUB -J mpiserial_test
#BSUB -q dev
#BSUB -W 00:01
#BSUB -x
if [[ -s /usrx/local/Modules/3.2.9/init/ksh ]] ; then
  . /usrx/local/Modules/3.2.9/init/ksh
fi
module load ics
module load ibmpe
export MP_TASK_AFFINITY=core
export MP_EUIDEVICE=sn_all
export MP_EUILIB=us

mpiserial=./mpiserial

echo ====== /bin/true
mpirun.lsf $mpiserial /bin/true
echo status $?

echo ====== /bin/false
mpirun.lsf $mpiserial /bin/false
echo status $?

echo ====== one rank false, others sleep and echo
cat<<EOF > cmdfile
/bin/false
/bin/sh -c 'echo start ; sleep 3 ; echo done ; exit 0'
/bin/sh -c 'echo start ; sleep 10 ; echo done ; exit 0'
EOF
mpirun.lsf $mpiserial
echo status $?

echo ===== get rank and comm size
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpirun.lsf $mpiserial
echo status $?

echo ===== too many ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 3 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpirun.lsf $mpiserial
echo status $?

echo ===== too few ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
mpirun.lsf $mpiserial
echo status $?

echo ===== try to run past wallclock limit
cat<<EOF > cmdfile
python -c 'while True: pass'
python -c 'while True: pass'
python -c 'while True: pass'
EOF
mpirun.lsf $mpiserial
echo status $?
