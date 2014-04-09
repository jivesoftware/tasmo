#!/bin/bash
echo "/-------------------------------------------------------"
echo "| checking running from develop branch. "
echo "\-------------------------------------------------------"
ON_BRANCH=`git branch | grep "^*" | awk '{ print $2}'`
if [ "$ON_BRANCH" != "develop" ]
then
	echo "You are release from branch '${ON_BRANCH}' which typically should be 'develop'. Are you sure you want to continue?r: y/n: "
	read ANSWER
	if [ "$?" -ne "0" ]; then
		exit 1;
	fi
	if [ "$ANSWER" != "y" ]
	then
		exit 1;
	fi
fi

echo "/-------------------------------------------------------"
echo "| checking for outstanding commits"
echo "\-------------------------------------------------------"
OUT_STANDING_COMMITS=`git status | grep "nothing to commit" | wc -l`
if [ "$OUT_STANDING_COMMITS" != "1" ]
then
	UNTRACKED=`git status | grep "nothing added to commit" | wc -l`
	if [ "$UNTRACKED" != "1" ]
	then
		git status		
		echo "You have untracked files. Do you want to release anyway?: y/n: "
		read ANSWER
		if [ "$?" -ne "0" ]; then
			cd -
			exit 1;
		fi
		if [ "$ANSWER" != "y" ]
		then
			cd -
			exit 1;
		fi
	else 
		cd -
		echo "/-------------------------------------------------------"
		echo "| you have outstanding commits. Release cannot proceed until all commits are pushed."
		echo "\-------------------------------------------------------"
		exit 1;	
	fi
	
fi

VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -v '\[' | tr '-' ' ' | awk '{ print $1 }'`


echo "/-------------------------------------------------------"
echo "| setting version to "${VERSION}
echo "\-------------------------------------------------------"
find . -name "pom.xml" | xargs -n 1 mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${VERSION} -pl
git add -A
git commit -m "release "${VERSION}
git push origin ${ON_BRANCH}
if [ "$?" -ne "0" ]; then
	echo "Failed to push release to "${ON_BRANCH}
	exit 1
fi

git checkout master
git merge ${ON_BRANCH}
if [ "$?" -ne "0" ]; then
	echo "Failed to merge to master."
	exit 1
fi

git push origin master
if [ "$?" -ne "0" ]; then
	echo "Failed to push to master."
	exit 1
fi


git checkout ${ON_BRANCH}
echo "/-------------------------------------------------------"
echo "| setting version to "${VERSION}"-SNAPSHOT on branch "${ON_BRANCH}
echo "\-------------------------------------------------------"
find . -name "pom.xml" | xargs -n 1 mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${1}-SNAPSHOT -pl
git add -A
git commit -m "begin "${1}"-SNAPSHOT"
git push origin ${ON_BRANCH}

