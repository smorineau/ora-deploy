create or replace function IBS_UTILS.qrysum(p_query in varchar2)
return varchar2
is
   type fmtlen_tab is table of number
                      index by binary_integer;
   type buffer_tab is table of dbms_sql.varchar2_table;
   i_c              integer;
   i_cnt            integer := 0;
   i_ret            integer;
   i_rowsfound      integer;
   n_rowlen         number := 0;
   n_rows           number;
   n_col            number;
   n_checksums      number;
   a_desc           dbms_sql.desc_tab;
   a_fmtlen         fmtlen_tab;
   a_data           buffer_tab := buffer_tab();
   r_checksum       raw(16) := null;
   r_checksumbatch  raw(32767);
   v_buffer         varchar2(32767);
begin
  if (p_query is not null)
  then
    --
    -- Get length for date/time types once converted to varchar
    --
    for rec in (select type_code, fmtlen
                from (select case parameter
                               when 'NLS_DATE_FORMAT'         then  12
                               when 'NLS_TIMESTAMP_FORMAT'    then 180
                               when 'NLS_TIMESTAMP_TZ_FORMAT' then 181
                               when 'NLS_TIME_FORMAT'         then 178
                               when 'NLS_TIME_TZ_FORMAT'      then 179
                               else -1
                             end type_code,
                             length(value) fmtlen
                      from NLS_SESSION_PARAMETERS
                      where parameter like '%FORMAT')
                where type_code <> -1)
    loop
      a_fmtlen(rec.type_code) := rec.fmtlen; 
    end loop;
    i_c := dbms_sql.open_cursor;
    dbms_sql.parse(i_c, p_query, dbms_sql.native);
    dbms_sql.describe_columns(i_c, i_cnt, a_desc);
    --
    -- First loop to estimate the length of one row after conversion
    -- to varchar
    --
    n_col := a_desc.first;
    while (n_col is not null)
    loop
      if (a_desc(n_col).col_type in (2,3,29)) then -- numbers
        n_rowlen := n_rowlen + 15;
      elsif (a_desc(n_col).col_type in (8,11,24,69,102,104,
                                        110,111,112,113,114,
                                        115,121,122,123,250,
                                        251,252)) then  -- unsupported
        raise_application_error(-20000,
                                'LONGs, LOBs, ROWIDs and complex columns are unsupported');
      elsif (a_desc(n_col).col_type = 23) then  -- raw
        n_rowlen := n_rowlen + 2 * a_desc(n_col).col_max_len;
      elsif (a_desc(n_col).col_type in (12,178,179,180,181,182,183,231)) then  -- date/time
             begin
               n_rowlen := n_rowlen + a_fmtlen(a_desc(n_col).col_type);
             exception
                when no_data_found then
                     n_rowlen := n_rowlen + 20;
             end;
      else
        n_rowlen := n_rowlen + a_desc(n_col).col_max_len;
      end if;
      n_col := a_desc.next(n_col);
    end loop; 
    --
    -- Compute how many rows we can store in 32K
    --
    n_rows := floor(32767/n_rowlen);
    --
    -- Second loop to associate each column to a varchar array buffer
    --
    for i in 1 .. i_cnt
    loop
      a_data.extend(1);
      dbms_sql.define_array(i_c, i,
                            a_data(i), n_rows, 1);
    end loop; 
    i_ret := dbms_sql.execute(i_c);
    n_checksums := 0;
    v_buffer := '';
    loop
      i_rowsfound := dbms_sql.fetch_rows(i_c);
      --
      --  Loop on all columns to fetch the values
      --  Note that we process column after column
      --  rather than row after row but it doesn't
      --  matter as long as we are consistent.
      --
      for i in 1 .. i_cnt
      loop 
        dbms_sql.column_value(i_c, i, a_data(i));
        for j in 1 .. i_rowsfound
        loop
          v_buffer := v_buffer || a_data(i)(j);
        end loop;
      end loop;
      --
      --  At this stage, we have stuffed as much data
      --  as we can in v_buffer. Let's checksum it.
      --
      if (n_checksums >= 2000)
      then
        -- Compute the checksum of the existing concatenation
        -- of checksums
        -- r_checksumbatch := dbms_obfuscation_toolkit.md5(input=>r_checksumbatch);
        r_checksum := dbms_crypto.hash(r_checksumbatch, dbms_crypto.hash_md5);
        n_checksums := 1;
        r_checksumbatch := r_checksum;
      end if;
      -- r_checksum := utl_raw.cast_to_raw(dbms_obfuscation_toolkit.md5(input_string=>v_buffer));
      r_checksum := dbms_crypto.hash(utl_raw.cast_to_raw(v_buffer), dbms_crypto.hash_md5);
      n_checksums := n_checksums + 1;
      r_checksumbatch := utl_raw.concat(r_checksumbatch, r_checksum);
      v_buffer := '';
      exit when i_rowsfound <> n_rows;
    end loop;
    -- Checksum the whole thing ...
    -- r_checksum := dbms_obfuscation_toolkit.md5(input=>r_checksumbatch);
    r_checksum := dbms_crypto.hash(r_checksumbatch, dbms_crypto.hash_md5);
    dbms_sql.close_cursor(i_c);
  end if;
  return rawtohex(r_checksum);
exception
  when others then
    if (dbms_sql.is_open(i_c))
    then
      dbms_sql.close_cursor(i_c);
    end if;
    raise;  -- propagate exception
end;
/
show error

--select qrysum('select * from transactions where rownum <= 5000')
--from dual;
