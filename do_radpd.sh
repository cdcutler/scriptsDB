#!/bin/bash
# Clean and Concat files for load to aws s3 then redshift DW ::  RADPD files clean append & Push
#     Requires :: psql client , awscli tools , and 7z for unzip
#
set -e
set -u
# Set these environmental variables to override them,
source /home/ec2-user/etl/py/connect/RedConnect.sh
source /home/ec2-user/etl/py/connect/AwsAccess.sh
#
dirCsv="{put datadir}"
dirWork="{put ouputdir}"
dirArc="{put download archivedir }"
dirS3="s3://{put loading s3 bucket name}"
copyargs=" ESCAPE REMOVEQUOTES delimiter ',' MAXERROR 1000 ACCEPTANYDATE ignoreblanklines acceptinvchars "
DT=$(date +%y%d%m)
#
if [ -f  $dirArc$DT"RADPDNewVisionZipped.zip" ] ;
then
        7z x $dirArc$DT"RADPDNewVisionZipped.zip" -aos -o"$dirCsv"
else
        echo "nope no radpd "
        exit
fi
#
cd $dirCsv
# Clean  Remove  unwanted lines and chars  based on file name pattern 
for file in *.RADPD.*.CSV; do
        dos2unix $file
        sed -i.bak "s/'//g" $file
        sed -i.bak 's/=//g' $file
        sed -n -i '/DBN,DAY OF DISCH,STUDENT NAME,SEX,BIRTH DTE,STUDENT ID,CUR GRD,CUR CLS,ADMISSION DTE,DIS CDE/!p' $file
        sed -n -i '/TOTAL NUMBER OF STUDENTS:/!p' $file
        sed -n -i '/=========================/!p' $file
        sed -i.bak 's/\/[A-Za-z]{1,1}//g' $file
        sed -i.bak '/^$/d' $file
        sed -i.bak "s/$/\,\,\,$(date +%y%d%m)/" $file  ## c 10/26/16
done ;

# Append files of mathicng pattern name 
cat *.RADPD.*.CSV > $dirWork"testRADPD-CAT.CSV"
# S3 push from working
aws s3 cp $dirWork"testRADPD-CAT.CSV" $dirS3"testRADPD-upload"
## PGSQL Copy from s3

RUN_PSQL="/usr/bin/psql -X --set AUTOCOMMIT=off --set ON_ERROR_STOP=on --single-transaction "
${RUN_PSQL} <<SQL
\o /home/ec2-user/etl/py/ats/radpd.txt

BEGIN;
TRUNCATE TABLE stag_ats.test_radpd ;
COMMIT;

BEGIN;
copy stag_ats.test_radpd from 's3://$dirS3/testRADPD-upload' CREDENTIALS 'aws_access_key_id=$ACCESS1;aws_secret_access_key=$ACCESS2' $copyargs ;
COMMIT;

SQL