---
layout: home
---

# Running on a cluster

I use a qsub file like this to run on a cluster.

```shell
#!/bin/bash
#$ -b y
#$ -M "stuart.watt@uhnresearch.ca"
#$ -q highmem.q

module load vep/83
module load tabix/0.2.6

~/perl5/perlbrew/perls/perl-5.20.1/bin/perl import_wrapper/import.pl --config import_wrapper/config/impact.yml --output /mnt/work1/users/pughlab/cBioportal/data/impact --overwrite
```

Note my sneaky use of my own Perl build, made using `perlbrew`. 
