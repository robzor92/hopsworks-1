#!/bin/bash
# Deploy the frontend to the glassfish home directory and run bower
export SERVER=run_tls2
export LAST="/tmp/deploy-frontend-timestamp"
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes ${SERVER} "cd /srv/hops/domains/domain1 && sudo chown -R glassfish:vagrant docroot && sudo chmod -R 775 *"
if [ -f "$LAST" ]
then
  for file in $(find ../hopsworks-web/yo/app/ -newer $LAST -type f)
  do
    FILE_PATH=/srv/hops/domains/domain1/docroot$(echo $file | awk -F 'yo' '{print $2}')
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes $file ${SERVER}:$FILE_PATH
  done
  for file in $(find ../hopsworks-web/yo/bower.json ../hopsworks-web/yo/.bowerrc -newer $LAST -type f)
  do
    FILE_PATH=/srv/hops/domains/domain1/docroot/app$(echo $file | awk -F 'yo' '{print $2}')
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes $FILE ${SERVER}:$FILE_PATH
  done
else
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -r ../hopsworks-web/yo/app/ ${SERVER}:/srv/hops/domains/domain1/docroot
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes ../hopsworks-web/yo/bower.json ${SERVER}:/srv/hops/domains/domain1/docroot/app
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o IdentitiesOnly=yes ../hopsworks-web/yo/.bowerrc ${SERVER}:/srv/hops/domains/domain1/docroot/app
fi
touch $LAST
