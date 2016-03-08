package UHN::Importer;

use strict;
use warnings;

use Carp;
use Text::CSV;

use File::Temp qw/ tempfile tempdir /;

use Parallel::ForkManager;

use VCF;
use UHN::BuildCommands;
use UHN::Samples;

use Log::Log4perl;
my $log = Log::Log4perl->get_logger('UHN::Importer');

## Stolen from scripts/vct2maf, so we can take a simple approach when merging all our
## data files.

# Define default MAF Header (https://wiki.nci.nih.gov/x/eJaPAQ) with our vcf2maf additions
my @maf_header = qw(
    Hugo_Symbol Entrez_Gene_Id Center NCBI_Build Chromosome Start_Position End_Position Strand
    Variant_Classification Variant_Type Reference_Allele Tumor_Seq_Allele1 Tumor_Seq_Allele2
    dbSNP_RS dbSNP_Val_Status Tumor_Sample_Barcode Matched_Norm_Sample_Barcode
    Match_Norm_Seq_Allele1 Match_Norm_Seq_Allele2 Tumor_Validation_Allele1 Tumor_Validation_Allele2
    Match_Norm_Validation_Allele1 Match_Norm_Validation_Allele2 Verification_Status
    Validation_Status Mutation_Status Sequencing_Phase Sequence_Source Validation_Method Score
    BAM_File Sequencer Tumor_Sample_UUID Matched_Norm_Sample_UUID HGVSc HGVSp HGVSp_Short Transcript_ID
    Exon_Number t_depth t_ref_count t_alt_count tumor_vaf n_depth n_ref_count n_alt_count normal_vaf all_effects
);

# Add extra annotation columns to the MAF in a consistent order
my @ann_cols = qw( Allele Gene Feature Feature_type Consequence cDNA_position CDS_position
    Protein_position Amino_acids Codons Existing_variation ALLELE_NUM DISTANCE STRAND SYMBOL
    SYMBOL_SOURCE HGNC_ID BIOTYPE CANONICAL CCDS ENSP SWISSPROT TREMBL UNIPARC RefSeq SIFT PolyPhen
    EXON INTRON DOMAINS GMAF AFR_MAF AMR_MAF ASN_MAF EAS_MAF EUR_MAF SAS_MAF AA_MAF EA_MAF CLIN_SIG
    SOMATIC PUBMED MOTIF_NAME MOTIF_POS HIGH_INF_POS MOTIF_SCORE_CHANGE IMPACT PICK VARIANT_CLASS
    TSL HGVS_OFFSET PHENO MINIMISED ExAC_AF ExAC_AF_AFR ExAC_AF_AMR ExAC_AF_EAS ExAC_AF_FIN
    ExAC_AF_NFE ExAC_AF_OTH ExAC_AF_SAS GENE_PHENO FILTER );
my @ann_cols_format; # To store the actual order of VEP data, that may differ between runs
push( @maf_header, @ann_cols );

sub build_import {
  my ($cfg) = @_;

  my $overwrite = $cfg->{overwrite};

  my $study_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_study.txt");
  my $mutations_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_mutations_extended.txt");
  my $clinical_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_clinical.txt");
  my $mutations_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_mutations_extended.txt");
  my $clinical_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_clinical.txt");

  my $commands = [];
  my $cases = {};
  build_commands($cfg, $commands);
  add_case_data($cfg, $cases, $commands);
  read_clinical_data($cfg, $cases, $commands);

  if ($overwrite || ! -e $clinical_data_file) {
    write_clinical_data($cfg, $clinical_data_file, $commands, $cases);
  }

  my %core_meta = ();
  $core_meta{cancer_study_identifier} =              $cfg->{cancer_study}->{identifier};

  my %study_meta = %core_meta;
  $study_meta{type_of_cancer} =                      $cfg->{cancer_study}->{type_of_cancer};
  $study_meta{name} =                                $cfg->{cancer_study}->{name};
  $study_meta{short_name} =                          $cfg->{cancer_study}->{short_name};
  $study_meta{description} =                         $cfg->{cancer_study}->{description};
  $study_meta{pmid} =                                $cfg->{cancer_study}->{pmid};
  $study_meta{groups} =                              $cfg->{cancer_study}->{groups};
  $study_meta{dedicated_color} =                     $cfg->{cancer_study}->{dedicated_color};

  my %mutations_meta = %core_meta;
  $mutations_meta{stable_id} =                       $mutations_meta{cancer_study_identifier}."_mutations";
  $mutations_meta{genetic_alteration_type} =         $cfg->{mutations}->{genetic_alteration_type};
  $mutations_meta{datatype} =                        $cfg->{mutations}->{datatype};
  $mutations_meta{show_profile_in_analysis_tab} =    $cfg->{mutations}->{show_profile_in_analysis_tab};
  $mutations_meta{profile_description} =             $cfg->{mutations}->{profile_description};
  $mutations_meta{profile_name} =                    $cfg->{mutations}->{profile_name};

  my %clinical_meta = %core_meta;
  $clinical_meta{stable_id} =                        $clinical_meta{cancer_study_identifier}."_clinical";
  $clinical_meta{genetic_alteration_type} =          $cfg->{clinical}->{genetic_alteration_type};
  $clinical_meta{datatype} =                         $cfg->{clinical}->{datatype};
  $clinical_meta{show_profile_in_analysis_tab} =     $cfg->{clinical}->{show_profile_in_analysis_tab};
  $clinical_meta{profile_description} =              $cfg->{clinical}->{profile_description};
  $clinical_meta{profile_name} =                     $cfg->{clinical}->{profile_name};

  write_meta_file($cfg, $study_meta_file, \%study_meta) if ($overwrite || ! -e $study_meta_file);
  write_meta_file($cfg, $mutations_meta_file, \%mutations_meta) if ($overwrite || ! -e $mutations_meta_file);
  write_meta_file($cfg, $clinical_meta_file, \%clinical_meta) if ($overwrite || ! -e $clinical_meta_file);

  ## Now generate the case lists...
  my $case_lists = $cfg->{case_lists};
  my @case_list_keys = keys %$case_lists;
  foreach my $case_list_key (@case_list_keys) {
    my $case_list_file = File::Spec->catfile($cfg->{OUTPUT}, "case_lists/cases_$case_list_key.txt");

    my %case_list = ();
    $case_list{cancer_study_identifier} =          $core_meta{cancer_study_identifier};
    $case_list{stable_id} =                        $case_list{cancer_study_identifier} . "_$case_list_key";
    $case_list{case_list_name} =                   $cfg->{case_lists}->{$case_list_key}->{name};
    $case_list{case_list_description} =            $cfg->{case_lists}->{$case_list_key}->{description};
    $case_list{case_list_ids} =                    join("\t", get_case_list_samples($cfg, $case_list_key, $commands));

    ## Case lists are essentially the same syntactically
    write_meta_file($cfg, $case_list_file, \%case_list) if ($overwrite || ! -e $case_list_file);
  }

  ## Do the mutations last as annotation takes a while...
  if ($overwrite || ! -e $mutations_data_file) {
    if (! $cfg->{_dry_run}) {
      execute_commands($cfg, $commands);
      write_extended_mutations_data($cfg, $mutations_data_file, $commands);
    }
  }
}

sub get_case_list_samples {
  my ($cfg, $case_list_key, $commands) = @_;

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

## If we don't have a clinical file, we should apply some different rules. Use patterns
## to derive patient identifiers from sample identifiers, and only write minimal clinical
## information.

sub add_case_data {
  my ($cfg, $cases, $commands) = @_;

  foreach my $command (@$commands) {
    my $patient = $command->{patient};
    my $sample = $command->{sample};

    my $source_key = $command->{options}->{source};
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

sub copy_hash {
  my ($target, $source) = @_;
  while(my ($k, $v) = each %$source) {
    $target->{$k} = $v;
  }
}

sub read_clinical_data {
  my ($cfg, $cases, $commands) = @_;

  ## Handle with care. The clinical data file might well be indexed only by
  ## patient identifiers, in which case it applies to all samples.

  my %patient_samples = ();
  foreach my $command (@$commands) {
    my $patient = $command->{patient};
    my $sample = $command->{sample};
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

sub build_commands {
  my ($cfg, $commands) = @_;

  $#$commands = -1;

  my $sources = $cfg->{sources};
  my @source_keys = keys %$sources;
  foreach my $source_key (@source_keys) {
    my $directory = $sources->{$source_key}->{directory} // croak("Missing directory configuration for source: $source_key");
    my $pattern = $sources->{$source_key}->{pattern} // $sources->{pattern} // $cfg->{source_pattern};
    my $origin = $sources->{$source_key}->{origin} // croak("Missing origin for source: $source_key");
    my @source_commands = UHN::BuildCommands::scan_paths($cfg, \&import_vcf_file, $pattern, $directory, $source_key, {type => $origin, source => $source_key});
    push @$commands, @source_commands;
  }

  return $commands;
}

sub execute_commands {
  my ($cfg, $commands) = @_;
  return if ($cfg->{_dry_run});

  my $pm = new Parallel::ForkManager($cfg->{max_processes});
  foreach my $command (@$commands) {
    my @args = ($command->{script}, @{$command->{arguments}});

    my $pid = $pm->start and next;
    $cfg->{LOGGER}->info("Processing file $command->{index}: $command->{description}");
    $cfg->{LOGGER}->info("Executing: " . join(" ", @args));

    system(@args) == 0 or do {
      $cfg->{LOGGER}->error("Command failed: $?");
      croak($?);
    };
    $cfg->{LOGGER}->info("Command completed: $command->{index}: $command->{description}");
    $pm->finish;
  }
  $pm->wait_all_children;

  $cfg->{LOGGER}->info("Done all commands");
}

sub write_clinical_data {
  my ($cfg, $output, $commands, $cases) = @_;

  $output = "/dev/null" if ($cfg->{_dry_run});

  my @headers = @{$cfg->{clinical_attributes}};
  push @headers, @{$cfg->{additional_clinical_attributes}};

  my $output_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";
  $output_fh->print("#" . join("\t", map { $_->{name} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{description} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{type} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{label} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{count} } @headers) . "\n");

  my @header_names = map { $_->{header} } @headers;
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
    my @values = map { $record{$_}; } @header_names;
    $output_fh->print(join("\t", @values) . "\n");
  }
}

sub write_extended_mutations_data {
  my ($cfg, $output, $commands) = @_;
  return if ($cfg->{_dry_run});

  $cfg->{LOGGER}->info("Merging MAF files into: $output");
  my @mafs = map { $_->{output} } (@$commands);

  my $maf_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";

  my $header1 = "#version 2.4\n";
  my $header2 = join("\t", @maf_header) . "\n";
  $maf_fh->print($header1 . $header2); # Print MAF header

  foreach my $maf (@mafs) {
    $cfg->{LOGGER}->info("Reading generated MAF: $maf");
    my $input_fh = IO::File->new($maf, "<") or carp "ERROR: Couldn't open input file: $maf!\n";
    while(<$input_fh>) {
      next if $_ eq $header1;
      next if $_ eq $header2;
      carp("Suspicious header: $_") if /^Hugo_Symbol/i;
      $maf_fh->print($_);
    }
    $input_fh->close();
  }
  $maf_fh->close();
}

sub import_vcf_file {
  my ($cfg, $base, $path, $source, $options) = @_;

  VCF::validate($path);

  $cfg->{_vcf_count} //= 1;

  my ($tumour, $normal) = UHN::Samples::get_sample_identifiers($cfg, $source, $path);
  if (! $tumour || ! $normal) {
    $cfg->{LOGGER}->error("Can't extract tumour/normal sample identifiers from: $path");
    croak("Can't extract tumour/normal sample identifiers from: $path");
  }

  my $sources = $cfg->{sources};
  my $tumour_sample_matcher = $sources->{$source}->{sample_matcher} // $sources->{sample_matcher} // $cfg->{tumour_sample_matcher};
  my $tumour_patient_generator = $sources->{$source}->{patient_generator} // $sources->{patient_generator} // $cfg->{tumour_patient_generator};

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
    ]
  };

  push @{$command->{arguments}}, '--no-vep-check-ref' if ($cfg->{no_vep_check_ref});

  return $command;
}

sub write_meta_file {
  my ($cfg, $file, $data) =  @_;
  $file = "/dev/null" if ($cfg->{_dry_run});
  open(my $fh, ">", $file) || croak("Can't open file: $file: $!");
  foreach my $key (sort keys %$data) {
    print $fh "$key: $data->{$key}\n";
  }
  close($fh);
}

1;
