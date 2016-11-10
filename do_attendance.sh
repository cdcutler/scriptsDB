#!/bin/bash
## PGSQL Copy from s3
set -e
set -u
# Set these environmental variables for creds , AWS & redshift
## NOTE: replace all text in < > with locaitons dir or filename ...
source <path to cred file>RedConnect.sh
source <path to cred file>/AwsAccess.sh
# SET ETL Var
dirCsv="/<input dir>/"
dirWork="/<output dir>/"
dirS3="s3://<put bucket name >/"
copyargs=" ESCAPE REMOVEQUOTES delimiter ',' MAXERROR 0 ACCEPTANYDATE ignoreheader 1 ignoreblanklines acceptinvchars "
staging.schema="stag_attendance"
staging.table="staging_student_attendance_daily"
prod.schema="attendance"
prod.table="t_student_attendance_daily"
history.table="h_student_attendance_daily"
run.id=$(date +$%y%m%d)
s3targetfile="'s3://<put bucket name / filename >' "
ATTDFILE="<put filename to process here>"
logfile="<put log file location >"
##
##
cd $dirCsv
for file in $ATTDFILE ; do
  dos2unix $file 
  DT=`date +%Y%m%d`
  cp $file $file_$DT
  sed -i "s/$/$DT/" $file  ## Add DT as Run.id to end of each line
  sed -i.bak 's/=//g' $file
  aws s3 cp $dirWork$ATTDFILE $s3targetfile
  aws s3 cp $dirWork$file$"_"$DT $dirS3"/archive/"$file"_"$DT
  echo $(wc -l $ATTDFILE) ;
done ;
RUN_PSQL="/usr/bin/psql -X --set AUTOCOMMIT=off --set ON_ERROR_STOP=on --single-transaction "
${RUN_PSQL} <<SQLCOPY
\o $logfile
BEGIN;
TRUNCATE TABLE $(staging.schema).$(staging.table) ;
COMMIT;
BEGIN;
copy $(staging.schema).$(staging.table) from $(s3targetfile) CREDENTIALS 'aws_access_key_id=$ACCESS1;aws_secret_access_key=$ACCESS2' $copyargs ;
COMMIT;
SQLCOPY
##
##
RUN_PSQL="/usr/bin/psql -X --set AUTOCOMMIT=off --set ON_ERROR_STOP=on --single-transaction "
$(RUN_PSQL) <<PROD
Drts: create unique , Create a changed table, insert new recs to prod, insert changed recs to history , update prod form changed
*/
CREATE TABLE #unique as
with uniattrec
AS
  (  select
                student_id,
                attendance_date as maxdate,
                last_name ,
                attendance_status_am
	from
                 $(staging.schema).$(staging.table) a
	except
 
  (select a.student_id, a.attendance_date as maxdate ,a.last_name ,a.attendance_status_am
                                               from   $(staging.schema).$(staging.table) b,  $(staging.schema).$(staging.table) a
                                               where a.student_id = b.student_id
                                               and a.attendance_date = b.attendance_date
                                               GROUP BY a.student_id,maxdate, a.Last_name , a.attendance_status_am
                                               HAVING   count(*) >1
                                               order by a.student_id, maxdate)
  )
  SELECT  distinct t.student_id as student_id
		, t.attendance_date as tattendance_date
       , s.student_id as sstudent_id
       , s.last_name
       , s.middle_name
       , s.first_name
       , s.birthdate as birth_date
       , s.ethnic_code
       , s.home_lang
       , s.gender as sex
       , s.geocode
       , s.student_dbn
       , s.official_class
       , s.grade
       , s.grade_level
       , s.spec_ed_flag as spec_ed_flg
       , s.status
       , s.admission_date as admission_dte
       , s.admission_code as admissioncde
       , s.disc_dte
       , s.disc_code as disccde
       , s.attendance_grade
       , s.attendance_grade_level
       , s.attendance_date
       , s.attendance_status_am
       , s.attendance_status_pm
       , s.attendance_status_of
       , $(run.id) AS runId
  FROM  uniattrec mx
  left join  $(staging.schema).$(staging.table) s ON mx.student_id = s.student_id and mx.maxdate = s.attendance_date
  left join  $(prod.schema).$(prod.table) t ON mx.student_id = t.student_id and mx.maxdate = t.attendance_date
;
/* Make changed rec table based on compare attrib join student id and attd date   */
CREATE TABLE #changed AS
  SELECT prod.id as prodId, stag.*
  FROM #unique stag INNER JOIN $(prod.schema).$(prod.table) prod
  ON	(stag.student_id = prod.student_id and stag.attendance_date = prod.attendance_date)
  WHERE
  prod.first_name <> stag.first_name OR
  prod.middle_name <> stag.middle_name OR
  prod.last_name <> stag.last_name OR
  prod.birth_date <> stag.birth_date OR
  prod.ethnic_cde <> stag.ethnic_code OR
  prod.home_lang <> stag.home_lang OR
  prod.sex <> stag.sex OR
  prod.geocde <> stag.geocode OR
  prod.student_dbn <> stag.student_dbn OR
  prod.official_class <> stag.official_class OR
  prod.grade <> stag.grade OR
  prod.grade_level <> stag.grade_level OR
  prod.spec_ed_flg <> stag.spec_ed_flg OR
  prod.status <>  stag.status OR
  prod.admission_dte <> stag.admission_dte OR
  prod.admissioncde <>  stag.admissioncde OR 
  prod.disc_dte <> stag.disc_dte OR
  prod.disccde <>  stag.disccde OR
  prod.attendance_grade <> stag.attendance_grade OR
  prod.attendance_grade_level <> stag.attendance_grade_level OR
  prod.attendance_date <> stag.attendance_date OR
  prod.attendance_status_am <> stag.attendance_status_am OR
  prod.attendance_status_pm <> stag.attendance_status_pm OR
  prod.attendance_status_of  <> stag.attendance_status_of
;
/* Insert New recs to prod  by null studentid in prod and null attendance_date in prod   */
INSERT INTO $(prod.schema).$(prod.table) (
 student_id
,last_name
,middle_name
,first_name
,birth_date
,ethnic_cde
,home_lang
,sex
,geocde
,student_dbn
,official_class
,grade
,grade_level
,spec_ed_flg
,status
,admission_dte
,admissioncde
,disc_dte
,disccde
,attendance_grade
,attendance_grade_level
,attendance_date
,attendance_status_am
,attendance_status_pm
,attendance_status_of
,attendance_status
,recon_flag
,runId)
 (SELECT   s.sstudent_id
       , s.last_name
       , s.middle_name
       , s.first_name
       , s.birth_date
       , s.ethnic_code
       , s.home_lang
       , s.sex
       , s.geocode
       , s.student_dbn
       , s.official_class
       , s.grade
       , s.grade_level
       , s.spec_ed_flg
       , s.status
       , s.admission_dte
       , s.admissioncde
       , s.disc_dte
       , s.disccde
       , s.attendance_grade
       , s.attendance_grade_level
       , s.attendance_date
       , s.attendance_status_am
       , s.attendance_status_pm
       , s.attendance_status_of
       , null
       , null
       , $(run.id)
FROM  #unique s
WHERE s.student_id IS NULL
and s.tattendance_date IS NULL
);
/* Insert to history table where a changed prod.id is in prod    */
INSERT INTO $(prod.schema).h_student_attendance_daily (
student_id
,last_name
,middle_name
,first_name
,birth_date
,ethnic_cde
,home_lang
,sex
,geocde
,student_dbn
,official_class
,grade
,grade_level
,spec_ed_flg
,status
,admission_dte
,admissioncde
,disc_dte
,disccde
,attendance_grade
,attendance_grade_level
,attendance_date
,attendance_status_am
,attendance_status_pm
,attendance_status_of
,attendance_status
,recon_flag
,runId
,t_id
,h_runid)
(SELECT prod.student_id,
  prod.first_name,
  prod.middle_name ,
  prod.last_name,
  prod.birth_date,
  prod.ethnic_cde ,
  prod.home_lang,
  prod.sex,
  prod.geocde,
  prod.student_dbn ,
  prod.official_class ,
  prod.grade ,
  prod.grade_level,
  prod.spec_ed_flg ,
  prod.status ,
  prod.admission_dte ,
  prod.admissioncde  ,
  prod.disc_dte,
  prod.disccde ,
  prod.attendance_grade,
  prod.attendance_grade_level ,
  prod.attendance_date ,
  prod.attendance_status_am ,
  prod.attendance_status_pm ,
  prod.attendance_status_of ,
  null ,
  null ,
  $(run.id),
  prod.id,
  changed.runid
FROM        #changed changed INNER JOIN $(prod.schema).t_student_attendance_daily prod
ON (changed.prodId = prod.id)
);
/* update records on prod by prodId existing in changed and prod  */
UPDATE $(prod.schema).t_student_attendance_daily
SET last_name = changed.last_name,
  first_name = changed.first_name,
  middle_name = changed.middle_name,
  birth_date = changed.birth_date,
  ethnic_cde = changed.ethnic_code,
  home_lang = changed.home_lang,
  sex = changed.sex,
  geocde = changed.geocode,
  student_dbn = changed.student_dbn,
  official_class = changed.official_class,
  grade = changed.grade,
  grade_level = changed.grade_level,
  spec_ed_flg = changed.spec_ed_flg,
  status = changed.status,
  admission_dte = changed.admission_dte,
  admissioncde  = changed.admissioncde,
  disc_dte = changed.disc_dte,
  disccde = changed.disc_dte,
  attendance_grade = changed.grade,
  attendance_grade_level = changed.attendance_grade_level,
  attendance_date = changed.attendance_date,
  attendance_status_am = changed.attendance_status_am,
  attendance_status_pm = changed.attendance_status_pm,
  attendance_status_of = changed.attendance_status_of,
  attendance_status = ( CASE WHEN changed.attendance_status_am = 'A' and changed.attendance_status_pm = ' ' then  ' '
        			WHEN changed.attendance_status_am = 'A' and changed.attendance_status_of = 'H' then  ' '
        			WHEN changed.attendance_status_pm = 'L' then  'L'
        			ELSE  changed.attendance_status_am
        		END),
  runId = $(run.id)
FROM #changed changed
where (changed.prodId = id)
;
UPDATE $(prod.schema).$(prod.table)
SET attendance_status =
( CASE WHEN attendance_status_am = 'A' and attendance_status_pm = ' ' then  ' '
	WHEN attendance_status_am = 'A' and attendance_status_of = 'H' then  ' '
	WHEN attendance_status_pm = 'L' then  'L'
	ELSE  attendance_status_am
	END)
FROM  $(prod.schema).$(prod.table)
where attendance_status is NULL;
UPDATE $(prod.schema).h_student_attendance_daily
SET attendance_status =
(CASE WHEN attendance_status_am = 'A' and attendance_status_pm = ' ' then  ' '
	WHEN attendance_status_am = 'A' and attendance_status_of = 'H' then  ' '
	WHEN attendance_status_pm = 'L' then  'L'
	ELSE attendance_status_am
	END)
FROM $(prod.schema).h_student_attendance_daily
where attendance_status is NULL;
PROD
#
#
## Python mail call
# python $