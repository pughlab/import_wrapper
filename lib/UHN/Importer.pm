package UHN::Importer;

use strict;
use warnings;

use Carp;

use UHN::BuildCommands;
use UHN::Samples;

sub build_import {
  my ($cfg) = @_;

  my $varscan_directory = $cfg->{varscan_directory} // croak("Missing varscan_directory configuration");
  my @commands = UHN::BuildCommands::scan_paths($cfg, \&import_varscan_file, $varscan_directory);

  foreach my $command (@commands) {
    my @args = ($command->{script}, @{$command->{arguments}});
    $cfg->{LOGGER}->info("Executing: " + join(" ", @args));
    system(@args);
  }
}

sub import_varscan_file {
  my ($cfg, $base, $path) = @_;

  my ($tumour, $normal) = UHN::Samples::get_sample_identifiers($path);

  ## Make a temporary file place

  my $command = {
    script => $cfg->{vcf2maf},
    arguments => [
      'input-vcf', $path,
      'tumor-id', $tumour,
      'normal-id', $normal,
      'vcf-tumor-id', $tumour,
      'vcf-normal-id', $normal,
      'vep-path', $cfg->{vep_path},
      'vep-data', $cfg->{vep_data},
      'vep-forks', $cfg->{vep_forks},
      'ref-fasta', $cfg->{ref_fasta},
      'output-maf', 'xxx',
    ]
  }
}

1;