package UHN::Importer;

use strict;
use warnings;

use Carp;
use Class::Inspector;
use Text::CSV;
use File::Spec;
use File::Path qw(make_path);

use Log::Log4perl;
my $log = Log::Log4perl->get_logger('UHN::Importer');

use Moose;

has cfg => (
  is => 'rw'
);

has plugins => (
  is => 'rw'
);

has logger => (
  is => 'rw'
);

has cache_directory => (
  is => 'rw'
);

with 'UHN::GeneMapping';
with 'UHN::ExecutionManager';

sub BUILD {
  my ($self, $cfg) = @_;
  $self->logger($log);
  $self->cfg($cfg);
  my $finder = Module::Pluggable::Object->new(search_path => 'UHN::Importer::Format', instantiate => 'new');
  $self->plugins([$finder->plugins()]);

  my $cache_directory = $cfg->{cache_directory} // 'cache';
  my $directory = File::Spec->rel2abs($cache_directory, File::Spec->curdir());
  make_path($directory);
  $self->cache_directory($directory);
};

sub run {
  my ($self) = @_;
  my $commands = [];
  my $cases = {};
  $self->build_commands($commands);
  $self->add_case_data($cases, $commands);
  $self->read_clinical_data($cases, $commands);

  $self->write_study_meta_file();
  $self->write_clinical_patient_data($cases, $commands);
  $self->write_clinical_sample_data($cases, $commands);
  $self->execute_commands($commands);

  foreach my $plugin (@{$self->plugins()}) {
    $plugin->finish($self, $commands);
  }

  $self->write_case_lists($cases, $commands);
}

sub write_case_lists {
  my ($self, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  ## Now generate the case lists...
  if (! exists($cfg->{case_lists})) {
    die("No case lists defined in configuration file");
  }
  my $case_lists = $cfg->{case_lists};

  my @case_list_keys = keys %$case_lists;
  foreach my $case_list_key (@case_list_keys) {
    my $case_list_file = File::Spec->catfile($cfg->{OUTPUT}, "case_lists/cases_$case_list_key.txt");

    my %case_list = ();
    $case_list{cancer_study_identifier} =          $cfg->{cancer_study}->{identifier};
    $case_list{stable_id} =                        "$case_list{cancer_study_identifier}_$case_list_key";
    $case_list{case_list_name} =                   $cfg->{case_lists}->{$case_list_key}->{name};
    $case_list{case_list_description} =            $cfg->{case_lists}->{$case_list_key}->{description};
    $case_list{case_list_ids} =                    join("\t", $self->get_case_list_samples($case_list_key, $commands));

    ## Case lists are essentially the same syntactically
    $self->write_meta_file($case_list_file, \%case_list);
  }
}

sub write_study_meta_file {
  my ($self) = @_;
  my $cfg = $self->cfg();
  my %meta = ();
  $meta{cancer_study_identifier} =             $cfg->{cancer_study}->{identifier};
  $meta{type_of_cancer} =                      $cfg->{cancer_study}->{type_of_cancer};
  $meta{name} =                                $cfg->{cancer_study}->{name};
  $meta{short_name} =                          $cfg->{cancer_study}->{short_name};
  $meta{description} =                         $cfg->{cancer_study}->{description};
  $meta{pmid} =                                $cfg->{cancer_study}->{pmid};
  $meta{groups} =                              $cfg->{cancer_study}->{groups};
  $meta{dedicated_color} =                     $cfg->{cancer_study}->{dedicated_color};

  my $study_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_study.txt");
  $self->write_meta_file($study_meta_file, \%meta);
}

sub write_clinical_patient_data {
  my ($self, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  my $clinical_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_clinical_patients.txt");
  my $clinical_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_clinical_patients.txt");

  if ($cfg->{overwrite} || ! -e $clinical_data_file) {
    my $columns = $self->write_clinical_patient_data_file($clinical_data_file, $cases, $commands);
    if ($columns > 1) {

      ## Weird issues with cBio, zero patient attributes is actually an error. So we don't generate
      ## a meta file and remove the data file.

      my %meta = ();
      $meta{cancer_study_identifier} =          $cfg->{cancer_study}->{identifier};
      $meta{genetic_alteration_type} =          $cfg->{clinical}->{genetic_alteration_type};
      $meta{datatype} =                         "PATIENT_ATTRIBUTES";
      $meta{data_filename} =                    $clinical_data_file;

      $self->write_meta_file($clinical_meta_file, \%meta);
    } else {
      unlink($clinical_data_file);
    }
  }
}

sub write_clinical_sample_data {
  my ($self, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  my $clinical_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_clinical_samples.txt");
  my $clinical_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_clinical_samples.txt");

  my %meta = ();
  $meta{cancer_study_identifier} =          $cfg->{cancer_study}->{identifier};
  $meta{genetic_alteration_type} =          $cfg->{clinical}->{genetic_alteration_type};
  $meta{datatype} =                         "SAMPLE_ATTRIBUTES";
  $meta{data_filename} =                    $clinical_data_file;

  $self->write_meta_file($clinical_meta_file, \%meta);

  if ($cfg->{overwrite} || ! -e $clinical_data_file) {
    $self->write_clinical_sample_data_file($clinical_data_file, $cases, $commands);
  }
}

sub write_clinical_patient_data_file {
  my ($self, $output, $cases, $commands) = @_;
  return $self->write_clinical_data_file("PATIENT", $output, $cases, $commands);
}

sub write_clinical_sample_data_file {
  my ($self, $output, $cases, $commands) = @_;
  return $self->write_clinical_data_file("SAMPLE", $output, $cases, $commands);
}

sub write_clinical_data_file {
  my ($self, $selector, $output, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  $output = "/dev/null" if ($cfg->{_dry_run});

  my @headers = @{$cfg->{clinical_attributes}};
  push @headers, @{$cfg->{additional_clinical_attributes}};

  my $output_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";
  $output_fh->print("#" . join("\t", map { $_->{name} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{description} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{type} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{count} } @headers) . "\n");

  my @selected_headers = ();
  foreach my $header (@headers) {
    if ($selector eq 'SAMPLE' && $header->{name} eq 'PATIENT_ID') {
      push @selected_headers, $header;
      next;
    }
    if ($header->{label} eq $selector) {
      push @selected_headers, $header;
      next;
    }
  };

  my @header_names = map { $_->{header}; } @selected_headers;
  $output_fh->print(join("\t", @header_names). "\n");

  my %pairs = ();
  foreach my $command (@$commands) {
    my $sample = $command->{sample};
    my $patient = $command->{patient};
    $pairs{"$sample\t$patient"} = 1;
  }

  foreach my $pair (sort keys %pairs) {
    my ($sample, $patient) = split("\t", $pair);
    my $case = $cases->{$sample} // do { carp("Can't find sample case data: $sample"); undef; };
    my %record = ();
    @record{@header_names} = map { defined($case) ? $case->{$_} : ""; } @header_names;
    $record{PATIENT_ID} = $patient;
    $record{SAMPLE_ID} = $sample;
    $record{OS_STATUS} = 'LIVING' if (defined($record{OS_STATUS}) && $record{OS_STATUS} eq 'ALIVE');
    $record{OS_STATUS} = 'DECEASED' if (defined($record{OS_STATUS}) && $record{OS_STATUS} eq 'DEAD');
    my @values = map { $record{$_} // ''; } @header_names;
    $output_fh->print(join("\t", @values) . "\n");
  }

  my $columns = @header_names;
  return $columns;
}

sub write_meta_file {
  my ($self, $output, $data) = @_;
  my $cfg = $self->cfg();
  unless ($cfg->{overwrite} || ! -e $output) {
    return;
  }

  $output = "/dev/null" if ($cfg->{_dry_run});
  open(my $fh, ">", $output) || croak("Can't open file: $output: $!");
  foreach my $key (sort keys %$data) {
    print $fh "$key: $data->{$key}\n";
  }
  close($fh);
}

sub build_commands {
  my ($self, $commands) = @_;
  my $cfg = $self->cfg();

  $#$commands = -1;

  my $sources = $cfg->{sources};
  my @source_keys = keys %$sources;
  foreach my $source_key (@source_keys) {
    my $directory = $sources->{$source_key}->{directory} // croak("Missing directory configuration for source: $source_key");
    my $pattern = $sources->{$source_key}->{pattern} // $sources->{pattern} // $cfg->{source_pattern};
    foreach my $plugin (@{$self->plugins()}) {
      my $source_data = $sources->{$source_key};
      if ($plugin->handles_source($self, $source_data)) {
        my @source_commands = $plugin->scan($self, $pattern, $directory, $source_data, {source => $source_key});
        push @$commands, @source_commands;
      }
    }
  }

  return $commands;
}

sub add_case_data {
  my ($self, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  foreach my $command (@$commands) {
    my $patient = $command->patient();
    my $sample = $command->sample();

    my $source_key = $command->options()->{source};
    my $attributes = $cfg->{sources}->{$source_key}->{attributes};
    if (! defined($attributes)) {
      $cases->{$sample} //= {};
      next;
    }

    while(my ($key, $value) = each %$attributes) {
      if (! ref($value)) {
        $cases->{$sample}->{$key} = $value;
      } elsif (ref($value) eq 'ARRAY') {
        foreach my $entry (@$value) {
          my ($k) = keys %$entry;
          my $v = $entry->{$k};
          if ($sample =~ m{$v}) {
            $cases->{$sample}->{$key} = $k;
            last;
          }
        }
      }
    }
  }
}

sub read_clinical_data {
  my ($self, $cases, $commands) = @_;
  my $cfg = $self->cfg();

  ## Handle with care. The clinical data file might well be indexed only by
  ## patient identifiers, in which case it applies to all samples.

  my %patient_samples = ();
  foreach my $command (@$commands) {
    my $patient = $command->patient();
    my $sample = $command->sample();
    push @{$patient_samples{$patient}}, $sample;
  }

  if ($cfg->{clinical_file}) {
    my $clinical_data = $cfg->{clinical_file};
    my $csv = Text::CSV->new({binary => 1}) or die "Cannot use CSV: ".Text::CSV->error_diag();
    $csv->sep_char("\t");
    open my $fh, "<:encoding(utf8)", $clinical_data or die "$clinical_data: $!";
    my $headers = $csv->getline($fh);
    while (my $row = $csv->getline($fh)) {
      my %record = ();
      @record{@$headers} = @$row;
      if (exists($record{SAMPLE_ID})) {
        copy_hash($cases->{$record{SAMPLE_ID}}, \%record);
      } elsif (exists($record{PATIENT_ID})) {
        my $samples = $patient_samples{$record{PATIENT_ID}};
        foreach my $sample_id (@$samples) {
          copy_hash($cases->{$sample_id}, \%record);
        }
      } else {
        croak("Clinical file record has neither a patient nor a sample identifier");
      }
    }
    $cfg->{_clinical_file} = 1;
  } else {
    $log->warn("No clinical data file: falling back to identifier mapping");
    foreach my $command (@$commands) {
      my $patient = $command->{patient};
      my $sample = $command->{sample};
      my %record = (PATIENT_ID => $patient, SAMPLE_ID => $sample);
      copy_hash($cases->{$record{SAMPLE_ID}}, \%record);
    }
    $cfg->{_clinical_file} = 0;
  }
}

sub copy_hash {
  my ($target, $source) = @_;
  while(my ($k, $v) = each %$source) {
    $target->{$k} = $v;
  }
}

sub get_case_list_samples {
  my ($self, $case_list_key, $commands) = @_;
  my $cfg = $self->cfg();

  my $samples = {};

  my $data = $cfg->{case_lists}->{$case_list_key}->{data};
  if (ref($data) eq 'HASH') {

    ## When we have an action, include all samples
    my ($action) = keys %$data;
    my $sources = $cfg->{case_lists}->{$case_list_key}->{data}->{$action};
    foreach my $source (@$sources) {
      foreach my $command (@$commands) {
        if ($command->{options}->{source} eq $source) {
          $samples->{$command->{sample}} = 1;
        }
      }
    }
  } else {

    ## Include just this sample
    foreach my $command (@$commands) {
      if ($command->{options}->{source} eq $data) {
        $samples->{$command->{sample}} = 1;
      }
    }
  }

  return sort keys %$samples;
}

1;
