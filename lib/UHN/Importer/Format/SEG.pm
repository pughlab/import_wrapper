package UHN::Importer::Format::SEG;

use strict;
use warnings;

use Carp;
use Moose;

with 'UHN::Format';

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
  $log->info("Scanning $directory");
  $self->scan_paths($importer, \&_import_file, $pattern, $directory, $source_data, @args)
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
  $self->write_segment_meta_file($importer, $commands);
}

my @seg_header = ("ID", "chrom", "loc.start", "loc.end", "num.mark", "seg.mean");

sub write_segment_data {
  my ($self, $importer, $output, $commands) = @_;
  my $cfg = $importer->cfg();

  die("No commands") if (! defined($commands));

  return if ($cfg->{_dry_run});

  $log->info("Merging SEG files into: $output");
  my @segs = map { ($_->isa('UHN::Importer::Command::SEG')) ? ($_->output()) : () } (@$commands);
  $DB::single = 1;

  my $seg_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";

  my $header1 = join("\t", @seg_header) . "\n";
  $seg_fh->print($header1); # Print SEG header

  foreach my $seg (@segs) {
    $log->info("Reading generated mutations data: $seg");
    my $input_fh = IO::File->new($seg, "<") or carp "ERROR: Couldn't open input file: $seg!\n";
    while(<$input_fh>) {
      next if $_ eq $header1;
      carp("Suspicious header: $_") if /^ID/i;
      $seg_fh->print($_);
    }
    $input_fh->close();
  }
  $seg_fh->close();
}

sub write_segment_meta_file {
  my ($self, $importer, $commands) = @_;
  my $cfg = $importer->cfg();

  my %meta = ();
  $meta{cancer_study_identifier} =         $cfg->{cancer_study}->{identifier};
  $meta{stable_id} =                       $meta{cancer_study_identifier}."_segment";
  $meta{genetic_alteration_type} =         $cfg->{segment}->{genetic_alteration_type};
  $meta{datatype} =                        $cfg->{segment}->{datatype};
  $meta{show_profile_in_analysis_tab} =    $cfg->{segment}->{show_profile_in_analysis_tab};
  $meta{profile_description} =             $cfg->{segment}->{profile_description};
  $meta{profile_name} =                    $cfg->{segment}->{profile_name};

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
