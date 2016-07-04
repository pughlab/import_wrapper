package UHN::GeneMapping;

use strict;
use warnings;

use Moose::Role;

use Set::IntervalTree;
use Text::CSV;

requires 'cfg';
requires 'logger';

has gene_mapping_table => (
  is => 'rw'
);

has gene_name_table => (
  is => 'rw',
  default => sub { {} }
);

has ensembl_to_refseq_table => (
  is => 'rw',
  default => sub { {} }
);

sub get_gene_mapping {
  my ($self) = @_;
  my $log = $self->logger();

  my $table = $self->gene_mapping_table();
  return $table if (defined $table);

  $log->info("Loading gene mapping table");
  my $cfg = $self->cfg();
  my $gene_mapping_file = $cfg->{gene_mapping_file};
  $gene_mapping_file = File::Spec->rel2abs($gene_mapping_file, $FindBin::Bin);

  my $csv = Text::CSV->new({binary => 1}) or die "Cannot use CSV: ".Text::CSV->error_diag();
  open my $fh, "<:encoding(utf8)", $gene_mapping_file or die "$gene_mapping_file: $!";

  my $name_table = $self->gene_name_table();
  my $ensembl_table = $self->ensembl_to_refseq_table();
  my $headers = $csv->getline($fh);

  $table = {};
  while (my $row = $csv->getline($fh)) {
    my ($ensembl, $chrom, $start, $end, $strand, $symbol, $entrez_gene_id) = @$row;
    $entrez_gene_id = $entrez_gene_id + 0;

    if (! exists($table->{$chrom})) {
      $table->{$chrom} = Set::IntervalTree->new();
    }

    $name_table->{$entrez_gene_id} = {chrom => $chrom, strand => $strand, start => $start, end => $end, symbol => $symbol, ensembl => $ensembl};
    $ensembl_table->{$ensembl} = $entrez_gene_id;
    $table->{$chrom}->insert({ensembl => $ensembl, chrom => $chrom, strand => $strand, start => $start, end => $end, symbol => $symbol, entrez_gene_id => $entrez_gene_id}, $start, $end);
  }

  $self->gene_mapping_table($table);
  return $table;
}

1;
