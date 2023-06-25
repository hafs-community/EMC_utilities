#! /bin/bash

verbose=no
if [[ "$1" == -v ]] ; then
    verbose=yes
    shift 1
fi

if [[ "$#" -lt 3 ]] ; then
    echo "Syntax: lustre-correct.ksh groupname days /dir/1 [/dir/2 [...] ]" 1>&2
    echo "This will delete files older than \"days\" days.  Non-integers are" 1>&2
    echo "okay but values less than 1 are refused." 1>&2
    exit 1
fi

if ( ! /usr/bin/which lustre-walker > /dev/null 2>&1 ) ; then
    echo "Cannot find lustre-walker in your \$PATH" 1>&2
    exit 1
fi

group="$1"
age="$2"
rstprod=rstprod
moreopt=-q
shift 2

if [[ "$verbose" == yes ]] ; then
    moreopt=-s
fi

if [[ ! ( "$age" -ge 1 ) ]] ; then
    echo "ERROR: $age is not >= 1"
fi

if [[ "$verbose" == yes ]] ; then
    echo Running: lustre-walker -g "$group" -r "$rstprod" -d "$age" "$@"
fi

lustre-walker -g "$group" -r "$rstprod" -d "$age" "$@"
