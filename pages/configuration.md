---
layout: home
---

# The configuration file

All the configuration is done with YAML, because it's readable, commentable, and not XML.

## Cancer study

A typical configuration for this looks like:

```yaml
cancer_study:
  identifier: 'impact_compact'
  name: 'IMPACT/COMPACT'
  description: 'test'
  type_of_cancer: 'mixed'
  groups: ''
  dedicated_color: 'Black'
  short_name: 'IMPACT/COMPACT'
```

The settings here are used in all the secondary meta files needed by the whole study, as well as in the main meta file. In particular, `cancer_study.identifier` is used as a root stable identifier, so that you don't usually need to worry about any other stable identifiers in the whole study.

## Sources

Most of the genomic data comes in a variety of different sources. Many of these require some significant processing before they are used. A typical example are the VCF files from Mutect and Varscan. These are all re-annotated using the Ensembl VEP tool, and bundled into a monolithic MAF file for import. The import wrapper takes care of all of these steps. However, there are a few subtle points relating to sample and patient identifiers, which we will come to shortly.

A typical source definition looks like this:

```yaml
sources:
  exome:
    directory: '/mnt/work1/users/pughlab/projects/PJC003/Mutect_VCF/output/PASS'
    pattern: '(?i)\.vcf$'
    origin: 'mutect'
    sample_matcher: '(?i)([^_]+)_([A-Z]+)_(Tumor|Normal)'
    patient_generator: '$1'
    tumour_sample: '(?i)_Tumor$'
    normal_sample: '(?i)_Normal$'
```

This actually defines one source, using VCFs and analyzed by Mutect.

The fields here are interpreted as follows:

 * `directory` -- the directory where the VCFs will be found
 * `pattern` -- a regular expression which filters the files in that directory
 * `origin` -- which program generated the files, typically `mutect` or `varscan`
 * `sample_matcher` -- a regular expression which matches a sample identifier. Bracketed groups can be used to find parts of the identifier, and these can be substituted into the `patient_generator` field
 * `patient_generator` -- a string, using dollar strings, which builds a patient identifier from the values matched in the `sample_matcher`
 * `tumour_sample` -- a regular expression which tests whether a sample identifier is for a tumour sample or not
 * `normal_sample` -- a regular expression which tests whether a sample identifier is for a normal sample or not

## Settings

There are a moderate number of other settings which can be adjusted, and which might need to be set depending on your environment. Many of these are used to make sure the Ensembl variant effect predictor can be run correctly, as its annotation is essential to proper import.

Normally, we'd make these settings in a `defaults.yml` file which is used to fill in any default settings. So site-wide settings are best set here, rather than in the configuration for an individual study, where they'll just create an additional maintenance burden.

These settings include:

 * `vep_path` -- where is the Ensembl VEP installed
 * `vep_data` -- where is the Ensembl VEP reading its data from
 * `vep_dir_plugins ` -- what is the plugins directory for VEP
 * `ref_fasta` -- where is the reference genome FASTA file for VEP
 * `vep_forks` -- how much should we let VEP fork? (default is 4)
 * `max_processes` -- how many files should we process in parallel? (default is 4)
 * `no_vep_check_ref` -- if true, turns off reference cehcking in VEP (false by default)

## Case lists

cBioPortal requires at least one case list file. These define what the samples are
for a given piece of data. The import wrapper actually generates these from sources
directly, so that you don't need to worry about these things. Essentially, for each
source, the import wrapper can generate a list of associated samples. The case lists
allow you to use a union of multiple sources, merging data from several sources if
you need to.

There should always be an `all` source, merging all the samples from all
sources, but you will also typically separate, e.g., samples used for different
kinds of analysis.

The case lists show up on the main query page, so a user can choose which set
of samples to analyze.

A typical case list definition looks like this:

```yaml
case_lists:
  all:
    name: 'All'
    description: 'All exome and targeted'
    data:
      union:
        - 'exome'
        - 'targeted'
```
