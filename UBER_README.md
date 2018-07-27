to compile, use command: mvn clean install package -P build-shaded-jar -DskipTests

to release, use job: https://ci.uberinternal.com/job/pinot-release/


===========
to merge upstream changes, you will need to first add github pinot repo as upstream:
$git remote add upstream git@github.com:linkedin/pinot.git

then merge our changes with upstream:
$git fetch --all
$git merge upstream/master
