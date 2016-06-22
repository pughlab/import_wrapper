#!/bin/sh

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "Usage: runImport.sh <data directory>"

if [ ! -d "$1" ]; then
  die "Data directory parameter should be a directory"
fi

#change this to the directory containing files to import
DATA_DIRECTORY=$1

## One-liner to find the directory of the script: see http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
CONTEXT_BASE=/Users/stuartw/git/cbioportal-upstream/src/main/etc

## And from this, derive the Spring context file
CONTEXT_FILE=$CONTEXT_BASE/applicationContext-dao.xml

## Make sure we're using Java 8
##module load java/8

## And finally, run the import
java -cp /Users/stuartw/git/cbioportal-upstream/scripts/target/scripts-1.2.0-SNAPSHOT.jar org.mskcc.cbio.portal.ImportWrapper $DATA_DIRECTORY $CONTEXT_FILE
