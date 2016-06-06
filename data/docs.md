### Query used to extract data

```
select g.stable_id,
       sr.name as chromosome,
	   g.seq_region_start as txStart,
       g.seq_region_end as txEnd,
       g.seq_region_strand as strand,
	   selected.name,
       selected.accession
from gene g
join (select ox.ensembl_id,
	   x.display_label as name,
       x.dbprimary_acc as accession
  from xref x
  join external_db xdb on x.external_db_id = xdb.external_db_id
  join object_xref ox ON ox.xref_id = x.xref_id
  where xdb.db_name = 'EntrezGene'
  and ox.ensembl_object_type = 'Gene'
  group by ox.ensembl_id
  having count(*) = 1) selected on g.gene_id = selected.ensembl_id
JOIN seq_region sr ON g.seq_region_id = sr.seq_region_id
ORDER BY chromosome, txStart;
```
