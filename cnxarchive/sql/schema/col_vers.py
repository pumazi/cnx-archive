import psycopg2
import psycopg2.extras
from mx import DateTime


def modules_in_col_rev(cur,colid):
    cur.execute('select moduleid,module_ident,version from modules where module_id = $1 order by revised limit 2', colid)
    res = cur.fetchall()
    start_date = res[0]['revised']
    stop_date = DateTime.now()
    if len(res) == 2:
        stop_date = res[1]['revised']

        



conn = psycopg2.connect('dbname=repository user=rhaptos port=5433')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
cur.execute("select moduleid,module_ident,version,major_version,minor_version,revised,name,portal_type from modules m  where m.moduleid in ( select moduleid from modules where module_ident  in (select modules_in_tree(uuid,version) from modules where moduleid='col11448' union select module_ident from modules where moduleid = 'col11448')) order by revised")
res = cur.fetchall()
col_minor=0
col_major=0
col_ident = '(none)'
for r in res:
  if r['portal_type'] == 'Module':
    col_minor+=1
  else:
    col_ident = r['module_ident']
    col_major+=1
    col_minor=1
  print col_ident,r['module_ident'],r['revised'],'%s.%s' % (col_major,col_minor)
