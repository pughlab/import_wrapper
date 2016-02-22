package UHN::Samples;

use strict;
use warnings;

use Carp;

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

  my $sources = $cfg->{sources};
  my $tumour_sample_pattern = $sources->{$source}->{tumour_sample} // $sources->{tumour_sample} // $cfg->{tumour_sample_pattern};
  my $normal_sample_pattern = $sources->{$source}->{normal_sample} // $sources->{normal_sample} // $cfg->{normal_sample_pattern};

  if ($tumour_sample_pattern =~ /\d+/) {
    $result[0] = $values[$tumour_sample_pattern];
  } else {
    $result[0] = (grep { $_ =~ qr/$tumour_sample_pattern/ } @values)[0];
  }

  if ($normal_sample_pattern =~ /\d+/) {
    $result[1] = $values[$normal_sample_pattern];
  } else {
    $result[1] = (grep { $_ =~ qr/$normal_sample_pattern/ } @values)[0];
  }

  return @result;
}

1;
