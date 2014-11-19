-- create DB_CI user

define DB_CI_USER=IBS_DEPLOY

drop sequence &DB_CI_USER..DEPLOY;
create sequence &DB_CI_USER..DEPLOY;

drop table &DB_CI_USER..DEPLOY_CHECKSUM;
create table &DB_CI_USER..DEPLOY_CHECKSUM(
ID_DEPLOY               integer primary key,
SCHEMA_NAME             varchar2(30),
CHECKSUM_TIMESTAMP      timestamp,
SCHEMA_CHECKSUM         varchar2(4000) ,--not null,
COLUMNS_CHECKSUM        varchar2(4000) ,--not null,
CONS_CHECKSUM           varchar2(4000) ,--not null,
PROCS_CHECKSUM          varchar2(4000) ,--not null,
COMMENTS                varchar2(4000)
);


create or replace package &DB_CI_USER..DB_CI_MGMT
is

function GET_SCHEMA_CHECKSUM(P_SCHEMA_NAME in varchar2) return varchar2;
function GET_COLUMNS_CHECKSUM(P_SCHEMA_NAME in varchar2) return varchar2;
function GET_CONS_CHECKSUM(P_SCHEMA_NAME in varchar2) return varchar2;
function GET_PROCS_CHECKSUM(P_SCHEMA_NAME in varchar2) return varchar2;
procedure INSERT_CHECKSUM(P_SCHEMA_NAME in varchar2, P_DEPLOY_ID in pls_integer default null, P_COMMENTS in varchar2 default null);
function SCHEMA_UNCHANGED(P_SCHEMA_NAME in varchar2) return boolean;

end DB_CI_MGMT;
/
show err

create or replace package body &DB_CI_USER..DB_CI_MGMT
is

/*****************************************************************************/

function GET_SCHEMA_CHECKSUM(P_SCHEMA_NAME in varchar2)
return varchar2
is
  l_schema_checksum             &DB_CI_USER..DEPLOY_CHECKSUM.SCHEMA_CHECKSUM%type;
begin
  l_schema_checksum := IBS_UTILS.qrysum('select OWNER, OBJECT_NAME, SUBOBJECT_NAME, OBJECT_ID, OBJECT_TYPE, CREATED, NAMESPACE from dba_objects where owner = ''' || P_SCHEMA_NAME || ''' order by OBJECT_NAME, SUBOBJECT_NAME, OBJECT_ID, OBJECT_TYPE' );
  return l_schema_checksum;
end GET_SCHEMA_CHECKSUM;

function GET_COLUMNS_CHECKSUM(P_SCHEMA_NAME in varchar2)
return varchar2
is
  l_columns_checksum            &DB_CI_USER..DEPLOY_CHECKSUM.COLUMNS_CHECKSUM%type;
begin
  l_columns_checksum := IBS_UTILS.qrysum('select OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH from dba_tab_columns where owner = ''' || P_SCHEMA_NAME || ''' order by OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE' );
  return l_columns_checksum;
end GET_COLUMNS_CHECKSUM;

function GET_CONS_CHECKSUM(P_SCHEMA_NAME in varchar2)
return varchar2
is
  l_cons_checksum               &DB_CI_USER..DEPLOY_CHECKSUM.CONS_CHECKSUM%type;
begin
  l_cons_checksum := IBS_UTILS.qrysum('select OWNER,CONSTRAINT_NAME,CONSTRAINT_TYPE,TABLE_NAME,R_OWNER,R_CONSTRAINT_NAME,DELETE_RULE,STATUS,DEFERRABLE,DEFERRED,VALIDATED,GENERATED,BAD,RELY from dba_constraints where owner = ''' || P_SCHEMA_NAME || ''' order by CONSTRAINT_NAME,CONSTRAINT_TYPE,TABLE_NAME' );
  return l_cons_checksum;
end GET_CONS_CHECKSUM;

function GET_PROCS_CHECKSUM(P_SCHEMA_NAME in varchar2)
return varchar2
is
  l_procs_checksum              &DB_CI_USER..DEPLOY_CHECKSUM.PROCS_CHECKSUM%type;
begin
  l_procs_checksum := IBS_UTILS.qrysum('select OWNER,NAME,TYPE,LINE,TEXT from dba_source where owner = ''' || P_SCHEMA_NAME || ''' order by NAME,TYPE,LINE' );
  return l_procs_checksum;
end GET_PROCS_CHECKSUM;

/*****************************************************************************/

function SCHEMA_UNCHANGED(P_SCHEMA_NAME in varchar2)
return boolean
is
  l_latest_schema_checksum             &DB_CI_USER..DEPLOY_CHECKSUM.SCHEMA_CHECKSUM%type;
  l_latest_columns_checksum            &DB_CI_USER..DEPLOY_CHECKSUM.COLUMNS_CHECKSUM%type;
  l_latest_cons_checksum               &DB_CI_USER..DEPLOY_CHECKSUM.CONS_CHECKSUM%type;
  l_latest_procs_checksum              &DB_CI_USER..DEPLOY_CHECKSUM.PROCS_CHECKSUM%type;
  
  l_current_schema_checksum             &DB_CI_USER..DEPLOY_CHECKSUM.SCHEMA_CHECKSUM%type;
  l_current_columns_checksum            &DB_CI_USER..DEPLOY_CHECKSUM.COLUMNS_CHECKSUM%type;
  l_current_cons_checksum               &DB_CI_USER..DEPLOY_CHECKSUM.CONS_CHECKSUM%type;
  l_current_procs_checksum              &DB_CI_USER..DEPLOY_CHECKSUM.PROCS_CHECKSUM%type;
begin
  select
         SCHEMA_CHECKSUM,
         COLUMNS_CHECKSUM,
         CONS_CHECKSUM,
         PROCS_CHECKSUM
    into
         l_latest_schema_checksum,
         l_latest_columns_checksum,
         l_latest_cons_checksum,
         l_latest_procs_checksum
    from
         &DB_CI_USER..DEPLOY_CHECKSUM
   where
         SCHEMA_NAME = P_SCHEMA_NAME
     and CHECKSUM_TIMESTAMP = (select max(CHECKSUM_TIMESTAMP)
                                 from &DB_CI_USER..DEPLOY_CHECKSUM
                                where SCHEMA_NAME = P_SCHEMA_NAME);

  l_current_schema_checksum  := GET_SCHEMA_CHECKSUM(P_SCHEMA_NAME);
  l_current_columns_checksum := GET_COLUMNS_CHECKSUM(P_SCHEMA_NAME);
  l_current_cons_checksum    := GET_CONS_CHECKSUM(P_SCHEMA_NAME);
  l_current_procs_checksum   := GET_PROCS_CHECKSUM(P_SCHEMA_NAME);
  
  if l_current_schema_checksum  = l_latest_schema_checksum  and
     l_current_columns_checksum = l_latest_columns_checksum and
     l_current_cons_checksum    = l_latest_cons_checksum    and
     l_current_procs_checksum   = l_latest_procs_checksum
  then
      return true;
  else
      return false;
  end if;

end SCHEMA_UNCHANGED;

/*****************************************************************************/

procedure INSERT_CHECKSUM(P_SCHEMA_NAME in varchar2, P_DEPLOY_ID in pls_integer default null, P_COMMENTS in varchar2 default null)
is
  pragma autonomous_transaction;
begin

  insert into &DB_CI_USER..DEPLOY_CHECKSUM
  select
         nvl(P_DEPLOY_ID, &DB_CI_USER..DEPLOY.nextval),
         P_SCHEMA_NAME,
         systimestamp,
         GET_SCHEMA_CHECKSUM(P_SCHEMA_NAME),
         GET_COLUMNS_CHECKSUM(P_SCHEMA_NAME),
         GET_CONS_CHECKSUM(P_SCHEMA_NAME),
         GET_PROCS_CHECKSUM(P_SCHEMA_NAME),
         P_COMMENTS
    from dual;
  commit;
end INSERT_CHECKSUM;

end DB_CI_MGMT;
/
show err
