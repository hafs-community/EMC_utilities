#! /bin/bash

verbose=no
if [[ "$1" == -v ]] ; then
    verbose=yes
    shift 1
fi

if [[ "$#" -lt 2 ]] ; then
    echo "Syntax: lustre-correct.ksh groupname /dir/1 [/dir/2 [...] ]" 1>&2
    exit 1
fi

if ( ! /usr/bin/which lustre-walker > /dev/null 2>&1 ) ; then
    echo "Cannot find lustre-walker in your \$PATH" 1>&2
    exit 1
fi

group="$1"
rstprod=rstprod
moreopt=-q
shift 1

if [[ "$verbose" == yes ]] ; then
    moreopt=-s
fi

if [[ "$verbose" == yes ]] ; then
    echo Running: lustre-walker $moreopt -g "$group" -r "$rstprod" "$@"
fi

lustre-walker $moreopt -g "$group" -r "$rstprod" "$@"
