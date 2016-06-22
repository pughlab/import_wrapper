---
layout: home
---

# Internals

## configuration

For a given study, the import wrapper reads data from three places:

 * Your specified YAML file (not versioned unless you do it)
 * `local.yml` in the root directory (not versioned)
 * `defaults.yml` in the root directory (versioned)

These are merged in order, so that your YAML file and `local.yml` take precedence. This means that you can put a copy of the import wrapper, and set up permanent preferences for file locations, for, e.g., the variant effect predictor, without having to copy these into each study's configuration file.

For example, on my development system, my `local.yml` reads as follows:

```yaml
vep_path: '/Users/stuartw/ensembl-tools-release-83/scripts/variant_effect_predictor'
vep_data: '/Users/stuartw/vep_cache'
vep_plugins: '/Users/stuartw/vep_plugins'
ref_fasta: '/Users/stuartw/83_GRCh37'

vep_forks: 4
vep_buffer_size: 200
no_vep_check_ref: true
max_processes: 0

## Merge in some options that were previously in vcf2maf, where they were hard to maintain
vep_extra_options: '--merged --polyphen b --gmaf --maf_1kg --maf_esp --plugin ExAC,/Users/stuartw/vep_plugins/data/ExAC.r0.3.sites.minus_somatic.vcf.gz'
```

This means I can debug stuff with local systems.
