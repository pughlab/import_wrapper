package UHN::Samples;

use strict;
use warnings;

use Carp;

## A small module that, given a Vcf, finds the samples as tumour, normal.
## This is from the "#CHROM header" line, and is essentially the
## columns after FORMAT.

sub get_sample_identifiers {
  my ($cfg, $file) = @_;
  if (! -f $file) {
    croak("File doesn't exist: $file: $!");
  }
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

  my @values = ();
  my @result = ();
  if (defined $header) {
    my @fields = split(/\s+/, $header);
    @values = @fields[9..$#fields];
  } else {
    return;
  }

  ## Now, we might select the tumour normal pairing by order, in which case
  ## it's easy.

  if ($cfg->{mapping}->{tumour_sample} =~ /\d+/) {
    $result[0] = $values[$cfg->{mapping}->{tumour_sample}];
  } else {
    $result[0] = (grep { $_ =~ qr/$cfg->{mapping}->{tumour_sample}/ } @values)[0];
  }

  if ($cfg->{mapping}->{normal_sample} =~ /\d+/) {
    $result[0] = $values[$cfg->{mapping}->{normal_sample}];
  } else {
    $result[0] = (grep { $_ =~ qr/$cfg->{mapping}->{normal_sample}/ } @values)[0];
  }

  return @result;
}

1;
