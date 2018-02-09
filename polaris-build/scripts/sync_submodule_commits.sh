#!/usr/bin/env bash

COMMIT_MESSAGE=$1
if [[ -z "${COMMIT_MESSAGE}" ]]
then
  COMMIT_MESSAGE="Sync up submodule commits"
fi


submodules="$(grep path .gitmodules | sed 's/.*= //')"
declare -a repos=(${submodules})
submodule_diffs="$(git diff --submodule)"

if [[ ! -z ${submodule_diffs} ]]
then

   echo "Submodule diffs were found.."
   echo "Finding submodules that need to be committed.."
   submodules=0
   for repo in "${repos[@]}"; do
      if [[ ! -z $(git status | grep "${repo}" | grep 'new commits') ]]
      then
        git add ${repo}
        submodules=$((submodules+1))
      fi
   done

   if ((${submodules} > 0 ))
   then
       echo "---------------------------------------"
       echo "Found ${submodules} submodules to commit.."
       echo "Commit Preview"
       git status
       echo "---------------------------------------"
       echo ${COMMIT_MESSAGE} > commit_message_file
       echo >> commit_message_file
       echo "${submodule_diffs}" >> commit_message_file

       echo "Commmiting.."
       cat commit_message_file

       git commit -F commit_message_file
       rm -f commit_message_file

   else
      echo "No new commits to sync."
      git diff --submodule
   fi
else 
   echo "No submodule diffs were found. Nothing to commit."
fi

