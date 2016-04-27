package UHN::Samples::SEG;

use strict;
use warnings;

use Carp;

use Log::Log4perl;
my $log = Log::Log4perl->get_logger('UHN::Samples::SEG');

## A small module that, given a Vcf, finds the samples as tumour, normal.
## This is from the "#CHROM header" line, and is essentially the
## columns after FORMAT.

sub get_sample_identifiers {
  my ($cfg, $source, $file) = @_;
  if (! -f $file) {
    croak("File doesn't exist: $file: $!");
  }
  open(my $fh, "<", $file) || croak("Can't open file: $file: $!");
  my $header;
  while(<$fh>) {
    chomp;
    if (! /^ID\s+/i) {
      $header = $_;
      last;
    }
  };
  close($fh);

  my @values = ();
  my @result = ();
  if (defined $header) {
    my @fields = split(/\t/, $header);
    @values = @fields;
    foreach my $value (@values) {
      if ($value =~ m{\s}) {
        my $original = $value;
        $value =~ s{^\s+}{};
        $value =~ s{\s+$}{};
        $log->error("Whitespace in a sample name: '$original' in $file; fixing to '$value'");
      }
    }
  } else {
    return;
  }

  @result = ($values[0]);
  
  return @result;
}

1;
