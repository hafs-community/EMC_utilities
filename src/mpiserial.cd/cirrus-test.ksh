#! /bin/ksh

#@ job_type = parallel
#@ total_tasks = 3
#@ node = 1
#@ parallel_threads = 1
#@ output = /ptmp/wx20st/cirrus-test.log
#@ error = /ptmp/wx20st/cirrus-test.log
#@ job_name = cirrustest
#@ class = dev
#@ group = dev
#@ account_no = HUR-T2O
#@ node_resources = ConsumableMemory(2 GB)
#@ wall_clock_limit = 00:10:00
#@ task_affinity = cpu(1)
#@ network.MPI=sn_all,not_shared,us
#@ queue
set -x
cd /ptmp/wx20st
mpiserial=/u/wx20st/emc-utils-1.0.0/src/mpiserial.cd/mpiserial

echo ===== get rank and comm size
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
poe $mpiserial
echo status $?

echo ====== one rank false, others sleep and echo
cat<<EOF > cmdfile
/bin/false
/bin/sh -c 'echo start ; sleep 3 ; echo done ; exit 0'
/bin/sh -c 'echo start ; sleep 10 ; echo done ; exit 0'
EOF
poe $mpiserial
echo status $?

echo ====== /bin/true
poe $mpiserial /bin/true
echo status $?

echo ====== /bin/false
poe $mpiserial /bin/false
echo status $?

echo ===== too many ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 2 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 3 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
poe $mpiserial
echo status $?

echo ===== too few ranks
cat<<EOF > cmdfile
/bin/sh -c 'echo rank 0 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
/bin/sh -c 'echo rank 1 has: \$SCR_COMM_RANK \$SCR_COMM_SIZE'
EOF
poe $mpiserial
echo status $?
