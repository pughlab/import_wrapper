---
layout: home
---

# What is the import wrapper?

The import wrapper is a command-line tool that helps package a set of pipeline data for easy import into cBioPortal. it creates all the various meta files, and makes sure that everything is packaged correctly. It also takes care of merging a large block of VCF files into a single MAF file with the correct columns for the portal.

# Using the import_wrapper script

Using the script is very simple, because almost all the interesting information is set in a configuration file. We'll come to that later. To run the script:

```shell
perl import.pl --config <config.yml> --output <directory>
```

Note that the import wrapper doesn't overwrite anything that is already there, so if you have new input data, it's best to remove the output directory to ensure it actually gets processed.
