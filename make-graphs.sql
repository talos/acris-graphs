DROP TABLE IF EXISTS small_master;
CREATE TABLE small_master AS
SELECT row_number() OVER (ORDER BY document_id) as id, document_id::bigint,
       good_through_date, document_date,
       document_amt, recorded_datetime, modified_date,
       doc_type
 FROM acris_real_property_master m
WHERE document_date > '2003-01-01' AND document_id LIKE '20%'
  AND good_through_date IS NOT NULL
GROUP BY document_id, good_through_date, document_date, document_amt,
  recorded_datetime, modified_date, doc_type
ORDER BY document_id;

DELETE FROM small_master USING small_master alias
  WHERE small_master.document_id = alias.document_id AND
    small_master.good_through_date < alias.good_through_date;
CREATE UNIQUE INDEX ON small_master (document_id);

-- small table of recent parties --
-- Thanks socrata, sometimes you slip in random pieces of JSON into a CSV
DROP TABLE IF EXISTS small_parties;
CREATE TABLE small_parties AS
SELECT row_number() OVER (ORDER BY document_id, party_type, name, addr1, addr2, county, city, state, zip) as id,
  document_id::BIGINT, party_type,
  TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(UPPER(SUBSTR(name, 1, 70)), '[^A-Z0-9\- ]', '', 'g'), ' +', ' ', 'g'
  )) as name,
 TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(UPPER(addr1 || E'\n' || addr2 || E'\n' || city || ', ' || state || ' ' || zip || E'\n' || country), '[^A-Z0-9\-\n, ]', '', 'g'), ' +', ' ', 'g'
  )) as address,
  addr1, addr2, country, city, state, zip,
  good_through_date
FROM acris_real_property_parties
WHERE document_id LIKE '20%' AND NOT document_id LIKE '%[%'
ORDER BY document_id, party_type, name, addr1, addr2, country, city, state,
  zip, good_through_date;
CREATE EXTENSION pg_trgm;
CREATE INDEX ON small_parties (document_id);
CREATE UNIQUE INDEX ON small_parties (id);

-- attempt to find duplicate names & counts
CREATE TABLE dupe_names AS
SELECT row_number() OVER () as id, name, count(*)
FROM small_parties
GROUP BY name
ORDER BY name;
CREATE UNIQUE INDEX ON dupe_names (name);
CREATE UNIQUE INDEX ON dupe_names (id);
--CREATE INDEX ON dupe_names USING GIN (name gin_trgm_ops);
--SELECT SET_LIMIT(0.9);

-- duplicate addresses & counts
CREATE TABLE dupe_addresses AS
SELECT row_number() OVER () as id, adress, count(*)
FROM small_parties
GROUP BY address
ORDER BY address;
CREATE UNIQUE INDEX ON dupe_addresses (address);
CREATE UNIQUE INDEX ON dupe_addresses (id);
--CREATE INDEX ON dupe_addresses USING GIN (address gin_trgm_ops);

DROP TABLE IF EXISTS small_legals;
CREATE TABLE small_legals AS
SELECT row_number() OVER (ORDER BY document_id, borough, block, lot, addr_unit) as id,
  document_id::BIGINT,
  borough * 1000000000 + block * 10000 + lot as bbl,
  addr_unit,
  (borough * 1000000000 + block * 10000 + lot)::TEXT ||
    COALESCE('_' || addr_unit, '') as bbladdr,
  good_through_date
FROM acris_real_property_legals
WHERE document_id LIKE '20%'
  AND borough IS NOT NULL AND block IS NOT NULL AND lot IS NOT NULL
ORDER BY document_id, borough * 1000000000 + block * 10000 + lot, addr_unit;
CREATE INDEX ON small_legals (document_id);
CREATE INDEX ON small_legals (bbl);



DROP TABLE IF EXISTS vertex;
CREATE TABLE vertex (
  id TEXT PRIMARY KEY NOT NULL,
  type TEXT,
  bbl BIGINT, -- for properties only
  addr_unit TEXT, -- for properties only
  name TEXT, -- for entities only
  address TEXT
);

DROP TABLE IF EXISTS edge;
CREATE TABLE edge (
  id TEXT PRIMARY KEY NOT NULL,
  source TEXT, -- REFERENCES vertex (id),
  target TEXT, -- REFERENCES vertex (id),
  type TEXT,
  document_id1 BIGINT,
  document_type1 TEXT,
  document_date1 DATE,
  document_amt1 MONEY,
  document_id2 BIGINT,
  document_type2 TEXT,
  document_date2 DATE,
  document_amt2 MONEY
);

-- insert property vertices
INSERT INTO vertex (id, type, bbl, addr_unit)
SELECT 'property_' || bbladdr,
  'property', MAX(bbl), MAX(addr_unit)
FROM small_legals l
GROUP BY bbladdr;

-- insert "name" vertices
INSERT INTO vertex (id, type, name)
SELECT 'name_' || name, 'name', name
FROM dupe_names n
WHERE name IS NOT NULL;

-- insert edges person -> property
INSERT INTO edge (id, source, target, type,
  document_id1, document_type1, document_date1, document_amt1)
SELECT 'document_' || m.document_id || '_property_' || l.bbladdr || '_name_' || p.name || p.party_type,
  'name_' || p.name,
  'property_' || l.bbladdr,
  'document_property_name', m.document_id, MAX(doc_type), MAX(document_date),
  MAX(document_amt)::numeric::money
FROM small_master m JOIN
     small_parties p ON (m.document_id = p.document_id) JOIN
     small_legals l ON (p.document_id = l.document_id)
WHERE name IS NOT NULL AND party_type IS NOT NULL AND bbladdr IS NOT NULL
GROUP BY m.document_id, l.bbladdr, p.name, p.party_type;

-- insert edges person -> person from transactions
INSERT INTO edge (id, source, target, type, document_id1, document_type1,
  document_date1, document_amt1)
SELECT 'document_' || m.document_id || '_name_' || p1.name
      || '_name_' || p2.name,
  'name_' || p1.name,
  'name_' || p2.name,
  'document_name_name', m.document_id, doc_type,
  m.document_date, m.document_amt::numeric::money
FROM small_master m JOIN
     small_parties p1 ON (m.document_id = p1.document_id) JOIN
     small_parties p2 ON (m.document_id = p2.document_id)
WHERE p1.party_type = '1' AND p2.party_type = '2'
  AND p1.name IS NOT NULL AND p2.name IS NOT NULL;

-- insert edges person -> person from name
INSERT INTO edge (id, source, target, record_type, document_id)

EXPLAIN ANALYZE
SELECT
  --'NP' || p1.id::TEXT || 'P' || p2.id::TEXT,
  --'P' || p1.id::TEXT,
  --'P' || p2.id::TEXT, 'N',
  (SIMILARITY(p1.name, p2.name) * 100)::INT
  , p1.name, p1.count, p2.name, p2.count
FROM dupe_names p1, dupe_names p2
WHERE p1.id != p2.id
  AND p1.name % p2.name
  --AND p1.name = p2.name
  AND p2.count > p1.count
LIMIT 100;


-- insert edges person -> person from address

-- insert edges person -> person from having
-- been on  separate but adjacent deeds
-- (previous buyer to next seller)
INSERT INTO edge (id, source, target, record_type)
SELECT p1id, p2id FROM (
SELECT
  p1.id p1id, p2.id p2id,
  --l1.bbl, m1.document_date::date, substr(p1.name, 1, 20),
  --m2.document_date::date, substr(p2.name, 1, 20),
  dense_rank() OVER (PARTITION BY
                     m1.document_id,
                     l1.id,
                     p1.name
                     ORDER BY m2.document_id
                  ) as rank
FROM small_master m1, small_legals l1, small_legals l2,
     small_master m2, small_parties p1, small_parties p2
WHERE m1.document_id = l1.document_id
  AND l1.bbl = l2.bbl
  AND COALESCE(l1.addr_unit, '') = COALESCE(l2.addr_unit, '')
  AND l2.document_id = m2.document_id
  AND m1.doc_type IN ('DEED', 'DEEDO')
  AND m2.doc_type IN ('DEED', 'DEEDO')
  AND m2.document_date > m1.document_date
  AND m1.document_id = p1.document_id
  AND m2.document_id = p2.document_id
  AND p1.party_type = '2'
  AND p2.party_type = '1'
  AND m1.id BETWEEN 0 AND 100000
ORDER BY m1.document_id, l1.id, p1.name, m2.document_id
) AS ss WHERE rank = 1
;
-- insert edges property -> property (?)

ALTER TABLE vertex ADD PRIMARY KEY (id);

ALTER TABLE edge ADD FOREIGN KEY (source) REFERENCES vertex (id);
ALTER TABLE edge ADD FOREIGN KEY (target) REFERENCES vertex (id);

/*
docker exec -it docker4data gosu postgres psql -c \
    "\copy (select name from small_parties) to stdout" \
    > small_party_names

pip install python-levenshtein nltk

f = open('small_party_names')

import re

@decorators.memoize
def normalize_word(word):
    return re.sub('[^a-z0-9]', '', word.lower())

def get_words(names):
    words = set()
    for name in names:
        for word in name.split():
            words.add(normalize_word(word))
    return list(words)

@decorators.memoize
def vectorspaced(title):
    title_components = [normalize_word(word) for word in title.split()]
    return numpy.array([
        word in title_components and not word in stopwords
        for word in words], numpy.short)

names = [f.next().strip() for _ in xrange(0, 1000)]
words = get_words(names)

cluster = GAAClusterer(500)
cluster.cluster([vectorspaced(name) for name in names if name])

classified_examples = [
    cluster.classify(vectorspaced(name)) for name in names
]

for cluster_id, name in sorted(zip(classified_examples, names)):
    print cluster_id, name

########

def d(coord):
    i, j = coord
    return 1 - jaro_distance(words[i], words[j])

numpy.triu_indices(len(words), 1)
numpy.apply_along_axis(d, 0, _)


*/

