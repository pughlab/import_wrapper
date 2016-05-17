package UHN::Commands::SEG;

use strict;
use warnings;

use Carp;
use Exporter;
use File::Spec;
use File::Temp qw/ tempfile tempdir /;

use VCF;
use UHN::Samples::SEG qw(get_sample_identifiers);

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(import_file);

sub import_file {
  my ($cfg, $base, $path, $source, $options) = @_;

  my ($tumour) = UHN::Samples::SEG::get_sample_identifiers($cfg, $source, $path);
  if (! $tumour) {
    $cfg->{LOGGER}->error("Can't extract tumour sample identifiers from: $path");
    croak("Can't extract tumour sample identifiers from: $path");
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

  my $command = {
    output_type => 'segments',
    patient => $patient,
    sample => $tumour,
    options => $options,
    executed => 1,
  };

  return $command;
}

1;
