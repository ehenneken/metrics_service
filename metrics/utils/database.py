'''
Created on Jan 15, 2015

@author: ehenneken
'''

from flask.ext.sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def get_refereed(bibcodes):
    rawSQL = "SELECT bibcode,refereed FROM metrics WHERE bibcode IN (%s)"
    SQL = rawSQL % ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    res = db.session.execute(SQL)
    return dict([(r.bibcode, r.refereed) for r in res])

def get_unique_citations(bibcodes):
    rawSQL = "SELECT COUNT(*) AS unique_citations FROM (SELECT DISTINCT UNNEST(citations) FROM metrics WHERE bibcode IN (%s)) AS count"
    SQL = rawSQL % ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    res = db.session.execute(SQL)
    try:
        Nuniq = [r.unique_citations for r in res][0]
    except:
        Nuniq = "NA"
    return Nuniq

def get_publication_histogram(bibcodes, normalized=False):
    if not normalized:
        rawSQL = "SELECT CAST(LEFT(bibcode,4) AS INTEGER) AS year, CAST(COUNT(*) AS INTEGER) AS freq FROM metrics WHERE bibcode IN (%s) GROUP BY year ORDER BY year"
    else:
        rawSQL = "SELECT year, SUM(norm) AS freq FROM (SELECT CAST(LEFT(bibcode,4) AS INTEGER) AS year, 1/CAST(author_num AS REAL) AS norm FROM metrics WHERE bibcode IN (%s)) foo GROUP BY year ORDER BY year"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % bibstr
    res = db.session.execute(SQL)
    return dict([(r.year, r.freq) for r in res])

def get_usage_histogram(bibcodes,usage_type='reads',normalized=False):
    if not normalized:
        rawSQL = "SELECT 1995+idx AS year, CAST(SUM(read) AS INTEGER) FROM (SELECT UNNEST(%s) AS read, generate_subscripts(%s,1) AS idx FROM metrics WHERE bibcode IN (%s) ORDER BY idx) foo GROUP BY idx ORDER BY idx"
    else:
        rawSQL = "SELECT 1995+idx AS year, CAST(SUM(read) AS REAL) FROM (SELECT author_num, UNNEST(%s)/CAST(author_num AS REAL) AS read, generate_subscripts(%s,1) AS idx FROM metrics WHERE bibcode IN (%s) ORDER BY idx) foo GROUP BY idx ORDER BY idx"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (usage_type,usage_type,bibstr)
    res = db.session.execute(SQL)
    return dict([(r.year, r.sum) for r in res])
    
def get_citation_histogram(bibcodes,refereed=False, normalized=False):
    datatype = 'citations'
    if refereed:
        datatype = 'refereed_citations'
    if not normalized:
        rawSQL = "SELECT LEFT(UNNEST(%s),4) AS year, count(*) FROM metrics WHERE bibcode IN (%s) GROUP BY year ORDER BY year"
    else:
        rawSQL = "SELECT year, SUM(norm) FROM (SELECT CAST(LEFT(UNNEST(%s),4) AS INTEGER) AS year, 1/CAST(author_num AS REAL) AS norm FROM metrics WHERE bibcode IN (%s)) foo GROUP BY year ORDER BY year"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (datatype,bibstr)
    res = db.session.execute(SQL)
    return [(int(r[0]), int(r[1])) for r in res]

def get_self_citations(bibcodes):
    rawSQL = "SELECT COUNT(*) AS selfcit FROM (SELECT UNNEST(citations) FROM metrics WHERE bibcode IN (%s) INTERSECT SELECT bibcode FROM metrics WHERE bibcode IN (%s)) AS foo"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (bibstr,bibstr)
    res = db.session.execute(SQL)
    try:
        Nself = [r.selfcit for r in res][0]
    except:
        Nself = 0
    return Nself

def get_iN_index(bibcodes,N,year):
    rawSQL = "SELECT COUNT(citenum) AS i_N FROM (SELECT bibcode, count(*) AS citenum FROM (SELECT bibcode, pub_year, cite_year FROM (SELECT bibcode, CAST(LEFT(bibcode,4) AS INTEGER) AS pub_year, CAST(LEFT(UNNEST(citations),4) AS INTEGER) AS cite_year FROM metrics WHERE bibcode IN (%s)) foo WHERE pub_year <= %s AND cite_year <= %s) bar GROUP BY bibcode) baz WHERE citenum >= %s"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (bibstr,year,year,N)
    res = db.session.execute(SQL)
    try:
        iN = [r.i_N for r in res][0]
    except:
        iN = 0
    return iN

def get_h_index(bibcodes,year):
    rawSQL = "SELECT MAX(rank) AS h FROM (SELECT rank, citenum FROM (SELECT row_number() OVER(ORDER BY citenum DESC) AS rank, * FROM (SELECT bibcode, count(*) AS citenum FROM (SELECT bibcode, pub_year, cite_year FROM (SELECT bibcode, CAST(LEFT(bibcode,4) AS INTEGER) AS pub_year, CAST(LEFT(UNNEST(citations),4) AS INTEGER) AS cite_year FROM metrics WHERE bibcode IN (%s)) foo WHERE pub_year <= %s AND cite_year <= %s) bar GROUP BY bibcode) baz ORDER BY citenum DESC) x WHERE rank <= citenum) y"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (bibstr,year,year)
    res = db.session.execute(SQL)
    try:
        h = [r.h for r in res][0]
    except:
        h = 0
    return h

def get_g_index(bibcodes,year):
    rawSQL = "SELECT SQRT(MAX(r2)) as g FROM (SELECT r2, cumul FROM (SELECT r2, sum(citenum) OVER (ORDER BY r2) AS cumul FROM (SELECT rank*rank AS r2, CAST(citenum AS INTEGER) FROM (SELECT row_number() OVER(ORDER BY citenum DESC) AS rank, * FROM (SELECT bibcode, count(*) AS citenum FROM (SELECT bibcode, pub_year, cite_year FROM (SELECT bibcode, CAST(LEFT(bibcode,4) AS INTEGER) AS pub_year, CAST(LEFT(UNNEST(citations),4) AS INTEGER) AS cite_year FROM metrics WHERE bibcode IN (%s)) foo WHERE pub_year <= %s AND cite_year <= %s) bar GROUP BY bibcode) baz ORDER BY citenum DESC) x) y ) z WHERE r2 <= cumul) zz"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (bibstr,year,year)
    res = db.session.execute(SQL)
    try:
        g = [r.g for r in res][0]
    except:
        g = 0
    return g

def get_tori_index(bibcodes,year):
    # First calculate the uncorrected Tori (includes self-citations)
    rawSQL = "SELECT SUM(CAST(data->>'ref_norm' AS REAL) / CAST(author_num AS REAL)) AS tori FROM (SELECT author_num,json_array_elements(rn_citation_data) AS data FROM metrics WHERE bibcode IN (%s)) AS foo WHERE CAST(LEFT(data->>'bibcode',4) AS INTEGER) <= %s"
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    SQL = rawSQL % (bibstr,year)
    try:
        res = db.session.execute(SQL)
        tori_uncor = float([r.tori for r in res][0])
    except:
        return 0.0
    # Next determine the self-citations
    rawSQL = "SELECT DISTINCT UNNEST(citations) FROM metrics WHERE bibcode IN (%s) INTERSECT SELECT bibcode FROM metrics WHERE bibcode IN (%s)"
    SQL = rawSQL % (bibstr,bibstr)
    try:
        result = db.session.execute(SQL)
        selfcits = [r[0] for r in result]
    except:
        return tori_uncor
    # Calculate the correction for these self-citations
    rawSQL = "SELECT data->>'bibcode' AS bbc, CAST(data->>'ref_norm' AS REAL) / CAST(author_num AS REAL) AS correction FROM (SELECT author_num,json_array_elements(rn_citation_data) AS data FROM metrics WHERE bibcode IN (%s)) AS foo WHERE data->>'bibcode' IN (%s)"
    selfstr= ",".join(map(lambda a: "\'%s\'"%a,selfcits))
    SQL = rawSQL % (bibstr,selfstr)
    try:
        result = db.session.execute(SQL)
        correction = sum([r.correction for r in result])
    except:
        correction = 0.0
    tori = tori_uncor - correction
    # Return the corrected Tori
    return tori

def get_basic_stats(bibcodes,data_type,refereed=False):
    bibstr = ",".join(map(lambda a: "\'%s\'"%a,bibcodes))
    column_type = 'INTEGER'
    if data_type == 'citations':
        column = 'citation_num'
        if refereed:
            column = 'refereed_citation_num'
        from_query = 'metrics WHERE bibcode IN (%s)' % bibstr
    elif data_type == 'papers':
        column = '1/CAST(author_num AS REAL)'
        from_query = 'metrics WHERE bibcode IN (%s)' % bibstr
        column_type = 'REAL'
    else:
        column = 'total_usage'
        from_query = '(SELECT bibcode, author_num, (SELECT SUM(s) FROM UNNEST(%s) s) AS total_usage FROM metrics WHERE bibcode IN (%s)) x WHERE total_usage > 0' % (data_type, bibstr)
    
    rawSQL = "DROP AGGREGATE IF EXISTS median(anyelement);\
       CREATE OR REPLACE FUNCTION _final_median(anyarray) \
       RETURNS float8 AS \
    $$ \
      WITH q AS \
      ( \
         SELECT val \
         FROM unnest($1) val \
         WHERE VAL IS NOT NULL \
         ORDER BY 1 \
      ), \
      cnt AS \
      ( \
        SELECT COUNT(*) AS c FROM q \
      ) \
      SELECT AVG(val)::float8 \
      FROM \
      ( \
        SELECT val FROM q \
        LIMIT  2 - MOD((SELECT c FROM cnt), 2) \
        OFFSET GREATEST(CEIL((SELECT c FROM cnt) / 2.0) - 1,0)  \
      ) q2; \
    $$ \
    LANGUAGE sql IMMUTABLE; \
     \
    CREATE AGGREGATE median(anyelement) ( \
      SFUNC=array_append, \
      STYPE=anyarray, \
      FINALFUNC=_final_median, \
      INITCOND='{}' \
    ); \
     \
    SELECT \
        median(%s)\
        ,CAST(AVG(%s) AS REAL)\
        ,CAST(SUM(%s) AS %s)\
        ,SUM(CAST(%s AS REAL)/CAST(author_num AS REAL))\
    FROM \
        %s;"
        
    SQL = rawSQL % (column,column,column,column_type,column,from_query)
    return [r for r in db.session.execute(SQL)][0]
