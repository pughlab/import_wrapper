package UHN::Importer::Format::SEG;

use strict;
use warnings;

use Carp;
use Moose;
use Text::CSV;
use Set::IntervalTree;

use FindBin qw($Bin);

with 'UHN::Format';

has gene_mapping_table => (
  is => 'rw'
);

has gene_name_table => (
  is => 'rw',
  default => sub { {} }
);

has sample_name_table => (
  is => 'rw',
  default => sub { {} }
);

has segment_data => (
  is => 'rw',
  default => sub { {} }
);

use Log::Log4perl;
my $log = Log::Log4perl->get_logger('UHN::Importer::Format::SEG');

sub BUILD {
  $log->info("Loading SEG format plugin");
}

sub handles_source {
  my ($self, $importer, $source) = @_;
  return defined($source->{format}) && $source->{format} eq 'seg'
}

sub scan {
  my ($self, $importer, $pattern, $directory, $source_data, @args) = @_;
  $self->get_gene_mapping($importer);
  $log->info("Scanning $directory");
  $self->scan_paths($importer, \&_import_file, $pattern, $directory, $source_data, @args)
}

sub get_gene_mapping {
  my ($self, $importer) = @_;

  my $table = $self->gene_mapping_table();
  return $table if (defined $table);

  $log->info("Loading gene mapping table");
  my $cfg = $importer->cfg();
  my $gene_mapping_file = $cfg->{gene_mapping_file};
  $gene_mapping_file = File::Spec->rel2abs($gene_mapping_file, $FindBin::Bin);

  my $csv = Text::CSV->new({binary => 1}) or die "Cannot use CSV: ".Text::CSV->error_diag();
  open my $fh, "<:encoding(utf8)", $gene_mapping_file or die "$gene_mapping_file: $!";

  my $name_table = $self->gene_name_table();
  my $headers = $csv->getline($fh);

  $table = {};
  while (my $row = $csv->getline($fh)) {
    my ($ensembl, $chrom, $start, $end, $strand, $symbol, $entrez_gene_id) = @$row;
    $entrez_gene_id = $entrez_gene_id + 0;

    if (! exists($table->{$chrom})) {
      $table->{$chrom} = Set::IntervalTree->new();
    }

    $name_table->{$entrez_gene_id} = {chrom => $chrom, strand => $strand, start => $start, end => $end, symbol => $symbol};
    $table->{$chrom}->insert({ensembl => $ensembl, chrom => $chrom, strand => $strand, start => $start, end => $end, symbol => $symbol, entrez_gene_id => $entrez_gene_id}, $start, $end);
  }

  $self->gene_mapping_table($table);
  return $table;
}

sub _import_file {
  my ($self, $importer, $pattern, $path, $source, $options) = @_;
  my $cfg = $importer->cfg();

  my ($tumour) = $self->get_sample_identifiers($importer, $options->{source}, $path);
  if (! $tumour) {
    $cfg->{LOGGER}->error("Can't extract tumour sample identifier from: $path");
    croak("Can't extract tumour sample identifier from: $path");
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

  my $command = UHN::Importer::Command::SEG->new();
  $command->output($path);
  $command->output_type('segments');
  $command->patient($patient);
  $command->sample($tumour);
  $command->index($cfg->{_vcf_count}++);
  $command->description("seg file for $patient $tumour $path");
  $command->options($options);
  $command->arguments([]);
  $command->executed(1);

  return $command;
}

sub finish {
  my ($self, $importer, $commands) = @_;
  my $cfg = $importer->cfg();

  my $segment_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_segments.txt");

  if ($cfg->{overwrite} || ! -e $segment_data_file) {
    if (! $cfg->{_dry_run}) {
      $self->write_segment_data($importer, $segment_data_file, $commands);
    }
  }
  $self->write_segment_meta_file($importer, $segment_data_file, $commands);

  $self->write_cnv_data($importer);
  $self->write_cnv_meta_file($importer);
}

my @seg_header = ("ID", "chrom", "loc.start", "loc.end", "num.mark", "seg.mean");

sub write_segment_data {
  my ($self, $importer, $output, $commands) = @_;
  my $cfg = $importer->cfg();

  die("No commands") if (! defined($commands));

  return if ($cfg->{_dry_run});

  $log->info("Merging SEG files into: $output");
  my @segs = map { ($_->isa('UHN::Importer::Command::SEG')) ? ($_->output()) : () } (@$commands);

  my $seg_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";

  my $header1 = join("\t", @seg_header) . "\n";
  $seg_fh->print($header1); # Print SEG header

  foreach my $seg (@segs) {
    $log->info("Reading generated mutations data: $seg");
    my $input_fh = IO::File->new($seg, "<") or carp "ERROR: Couldn't open input file: $seg!\n";
    while(<$input_fh>) {
      next if $_ eq $header1;
      carp("Suspicious header: $_") if /^ID/i;

      ## Now remove a chr prefix from the second column, if it's there
      chomp;
      my @entries = split(/\t/, $_);
      $entries[1] =~ s{^chr(\w+)}{$1};
      $self->add_segment($importer, @entries);

      $seg_fh->print(join("\t", @entries)."\n");
    }
    $input_fh->close();
  }
  $seg_fh->close();
}

sub add_segment {
  my ($self, $importer, @entries) = @_;
  my $gene_mapping_table = $self->get_gene_mapping($importer);
  my $segment_data = $self->segment_data();
  my $sample_name_table = $self->sample_name_table();

  my ($sample, $chrom, $start, $end, $mark, $mean) = @entries;
  $sample_name_table->{$sample} = 1;

  my $set = $gene_mapping_table->{$chrom};

  my $genes = $set->fetch($start, $end);
  foreach my $gene (@$genes) {
    my $entrez_gene_id = $gene->{entrez_gene_id};
    push @{$segment_data->{$entrez_gene_id}->{$sample}}, [$start, $end, $mean];
  }
}

sub write_cnv_meta_file {
  my ($self, $importer) = @_;
  my $cfg = $importer->cfg();

  my %meta = ();
  $meta{cancer_study_identifier} =         $cfg->{cancer_study}->{identifier};
  $meta{stable_id} =                       $meta{cancer_study_identifier}."_log2CNA";
  $meta{genetic_alteration_type} =         $cfg->{cnv}->{genetic_alteration_type};
  $meta{datatype} =                        $cfg->{cnv}->{datatype};
  $meta{show_profile_in_analysis_tab} =    $cfg->{cnv}->{show_profile_in_analysis_tab};
  $meta{profile_description} =             $cfg->{cnv}->{profile_description};
  $meta{profile_name} =                    $cfg->{cnv}->{profile_name};

  my $mutations_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_log2cna.txt");
  $importer->write_meta_file($mutations_meta_file, \%meta);
}

sub write_cnv_data {
  my ($self, $importer) = @_;
  my $cfg = $importer->cfg();

  my $mutations_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_log2cna.txt");
  open my $fh, ">", $mutations_data_file or die("$mutations_data_file: $!");

  my $segment_data = $self->segment_data();

  my $sample_name_table = $self->sample_name_table();
  my $gene_name_table = $self->gene_name_table();
  my @samples = sort keys %$sample_name_table;
  my @genes = sort { $a <=> $b} keys %$segment_data;

  my @headers = qw(Hugo_Symbol Entrez_Gene_Id);
  push @headers, @samples;
  print $fh join("\t", @headers)."\n";

  for my $gene (@genes) {
    my $gene_name = $gene_name_table->{$gene};
    my $entries = $segment_data->{$gene};
    my @results = ();
    push @results, $gene_name->{symbol}, $gene;
    for my $sample (@samples) {
      $DB::single = 1 if (! $entries);
      if (exists($entries->{$sample})) {
        my @entries = @{$entries->{$sample}};
        push @results, $entries[0]->[2];
      } else {
        push @results, "";
      }
    }
    print $fh join("\t", @results)."\n";
  }
  close($fh);

  return;
}

sub write_segment_meta_file {
  my ($self, $importer, $segment_data_file, $commands) = @_;
  my $cfg = $importer->cfg();

  my %meta = ();
  $meta{cancer_study_identifier} =         $cfg->{cancer_study}->{identifier};
  $meta{stable_id} =                       $meta{cancer_study_identifier}."_segment";
  $meta{genetic_alteration_type} =         $cfg->{segment}->{genetic_alteration_type};
  $meta{datatype} =                        $cfg->{segment}->{datatype};
  $meta{show_profile_in_analysis_tab} =    $cfg->{segment}->{show_profile_in_analysis_tab};
  $meta{description} =                     $cfg->{segment}->{description};
  $meta{profile_name} =                    $cfg->{segment}->{profile_name};
  $meta{reference_genome_id} =             $cfg->{segment}->{reference_genome_id};
  $meta{data_filename} =                   $segment_data_file;

  my $mutations_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_segments.txt");
  $importer->write_meta_file($mutations_meta_file, \%meta);
}

sub get_sample_identifiers {
  my ($self, $importer, $source, $file) = @_;
  my $cfg = $importer->cfg();

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

package UHN::Importer::Command::SEG;

use strict;
use warnings;

use Moose;

with 'UHN::Command';

__PACKAGE__->meta->make_immutable;

1;
