package UHN::Samples;

use strict;
use warnings;

use Carp;

## A small module that, given a Vcf, finds the samples named in order.
## This is from the "#CHROM header" line, and is essentially the
## columns after FORMAT.

sub get_sample_identifiers {
  my ($file) = @_;
  open(my $fh, "<", $file) || croak("Can't open file: $file: $!");
  my $header;
  while(<$fh>) {
    chomp;
    if (/^#CHROM\s+/) {
      $header = $_;
      last;
    }
  };
  close($fh);
  if (defined $header) {
    my @fields = split(/\s+/, $header);
    return @fields[9..$#fields];
  };
  return;
}

1;
