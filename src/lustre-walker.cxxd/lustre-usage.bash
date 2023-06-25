#! /bin/bash

verbose=no
file_list=no

while [[ "$#" -gt 0 ]] ; do
    if [[ "$1" == -v ]] ; then
        verbose=yes
        shift 1
    elif [[ "$1" == -F ]] ; then
        file_list=yes
        shift 1
    else
        break
    fi
done

if [[ "$#" -lt 1 ]] ; then
    echo "Syntax: lustre-usage.ksh [-F] [-v] /dir/1 [/dir/2 [...] ]" 1>&2
    echo "  This script will generate usage statistics about all the specified" 1>&2
    echo "  directories.  It will subcategorize by user, group and directory," 1>&2
    echo "  including /dir/1, /dir/1/save, /dir/1/noscrub and /dir/1/scrub (if" 1>&2
    echo "  they exist)." 1>&2
    echo "Options:" 1>&2
    echo "  -v -- verbose" 1>&2
    echo "  -F -- also generate an all-files.txt with a listing of all files" 1>&2
    echo "        and their size, mtime, atime, etc." 1>&2
    exit 1
fi

if ( ! /usr/bin/which lustre-walker > /dev/null 2>&1 ) ; then
    echo "Cannot find lustre-walker in your \$PATH" 1>&2
    exit 1
fi

args=()

for dir in "$@" ; do
    for subdir in save noscrub scrub ; do
        if [[ -d "$dir/$subdir" ]] ; then
            args+=('-u')
            args+=("$dir/$subdir")
        fi
    done
    args+=('-u')
    args+=("$dir")
done

if [[ "$verbose" == yes ]] ; then
    args+=('-s') # print stats
else
    args+=('-q') # quiet mode
fi

if [[ "$file_list" == yes ]] ; then
    args+=('-F')
fi

for dir in "$@" ; do
    args+=("$dir")
done

if [[ "$verbose" == yes ]] ; then
    echo Running: lustre-walker "${args[@]}"
fi

lustre-walker "${args[@]}"
