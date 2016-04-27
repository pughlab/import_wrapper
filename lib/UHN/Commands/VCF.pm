package UHN::Commands::VCF;

use strict;
use warnings;

use Carp;
use Exporter;
use File::Spec;
use File::Temp qw/ tempfile tempdir /;

use VCF;
use UHN::Samples::VCF qw(get_sample_identifiers);

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(import_file);

sub import_file {
  my ($cfg, $base, $path, $source, $options) = @_;

  $DB::single = 1;
  VCF::validate($path);

  $cfg->{_vcf_count} //= 1;

  my $origin = $source->{origin} // die("Missing origin for source: $options->{source}");
  $options->{origin} = $origin;

  my ($tumour, $normal) = UHN::Samples::VCF::get_sample_identifiers($cfg, $options->{source}, $path);
  if (! $tumour || ! $normal) {
    $cfg->{LOGGER}->error("Can't extract tumour/normal sample identifiers from: $path");
    croak("Can't extract tumour/normal sample identifiers from: $path");
  }

  my $sources = $cfg->{sources};
  my $tumour_sample_matcher = $source->{sample_matcher} // $sources->{sample_matcher} // $cfg->{tumour_sample_matcher};
  my $tumour_patient_generator = $source->{patient_generator} // $sources->{patient_generator} // $cfg->{tumour_patient_generator};

  my $patient = $tumour;
  if ($patient =~ s{$tumour_sample_matcher}{$tumour_patient_generator}ee) {
    ## Good to go
  } else {
    die("Can't match sample pattern: " . $tumour_sample_matcher . ", original: " . $patient);
  }

  ## Make a temporary file place, but we need to track this, because we
  ## are going to need this file...

  my $directory = "$cfg->{TEMP_DIRECTORY}";
  my ($temp_fh, $temp_filename) = tempfile( "import_vcf_XXXXXXXXX", SUFFIX => '.maf', DIR => $directory);
  $temp_fh->close();
  unlink($temp_filename);

  my $script_path = File::Spec->rel2abs($cfg->{vcf2maf}, $FindBin::Bin);

  my $command = {
    script => $script_path,
    output => $temp_filename,
    output_type => 'mutations',
    patient => $patient,
    sample => $tumour,
    index => $cfg->{_vcf_count}++,
    description => "vcf2maf for $patient $tumour $path",
    options => $options,
    arguments => [
      '--input-vcf', $path,
      '--tumor-id', $tumour,
      '--normal-id', $normal,
      '--vcf-tumor-id', $tumour,
      '--vcf-normal-id', $normal,
      '--vep-path', $cfg->{vep_path},
      '--vep-data', $cfg->{vep_data},
      '--vep-forks', $cfg->{vep_forks},
      '--ref-fasta', $cfg->{ref_fasta},
      '--vep-buffer-size', $cfg->{vep_buffer_size},
      '--output-maf', $temp_filename,
      '--vep-extra-options', ($cfg->{vep_extra_options} // '')
    ]
  };

  push @{$command->{arguments}}, '--no-vep-check-ref' if ($cfg->{no_vep_check_ref});
  push @{$command->{arguments}}, '--vep-extra-options', $cfg->{vep_extra_options} if ($cfg->{vep_extra_options});

  return $command;
}

1;
