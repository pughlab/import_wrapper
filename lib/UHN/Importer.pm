package UHN::Importer;

use strict;
use warnings;

use Carp;

use File::Temp qw/ tempfile tempdir /;

use Parallel::ForkManager;

use UHN::BuildCommands;
use UHN::Samples;

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
    Exon_Number t_depth t_ref_count t_alt_count n_depth n_ref_count n_alt_count all_effects
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
  my $case_list_all_file = File::Spec->catfile($cfg->{OUTPUT}, "case_lists/cases_all.txt");

  my $commands = [];
  my $cases = {};
  build_commands($cfg, $commands);
  read_clinical_data($cfg, $cases);

  if ($overwrite || ! -e $clinical_data_file) {
    write_clinical_data($cfg, $clinical_data_file, $commands, $cases);
  }

  if ($overwrite || ! -e $mutations_data_file) {
    execute_commands($cfg, $commands);
    $cfg->{LOGGER}->info("Merging MAF files into: $mutations_data_file");
    my @mafs = map { $_->{output} } (@$commands);
    write_extended_mutations_data($cfg, $mutations_data_file, @mafs);
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

  ## Get all patient identifiers sequenced
  my $patients = {};
  foreach my $command (@$commands) {
    $patients->{$command->{sample}} = 1;
  }
  $patients = join("\t", sort keys %$patients);
  my %case_list_all = ();
  $case_list_all{cancer_study_identifier} =          $core_meta{cancer_study_identifier};
  $case_list_all{stable_id} =                        $case_list_all{cancer_study_identifier} . "_all";
  $case_list_all{case_list_name} =                   $cfg->{case_lists}->{all}->{name};
  $case_list_all{case_list_description} =            $cfg->{case_lists}->{all}->{description};
  $case_list_all{case_list_ids} =                    $patients;

  ## Now we can do the unbelievable task of building a new file which contains the
  ## MAF output of every single of these, merged.

  write_meta_file($study_meta_file, \%study_meta) if ($overwrite || ! -e $study_meta_file);
  write_meta_file($mutations_meta_file, \%mutations_meta) if ($overwrite || ! -e $mutations_meta_file);
  write_meta_file($clinical_meta_file, \%clinical_meta) if ($overwrite || ! -e $clinical_meta_file);

  ## Case lists are essentially the same syntactically
  write_meta_file($case_list_all_file, \%case_list_all) if ($overwrite || ! -e $case_list_all_file);
}

sub read_clinical_data {
  my ($cfg, $cases) = @_;

  my $clinical_data = $cfg->{clinical_file} // croak("Missing clinical_file configuration");
  my $csv = Text::CSV->new({binary => 1, sep_char => '\t'}) or die "Cannot use CSV: ".Text::CSV->error_diag();
  open my $fh, "<:encoding(utf8)", $clinical_data or die "$clinical_data: $!";
  my $headers = $csv->getline($fh);
  while (my $row = $csv->getline($fh)) {
    my %record = ();
    @record{@$headers} = @$row;
    $cases->{$record{PATIENT_ID}} = \%record;
  }
}

sub build_commands {
  my ($cfg, $commands) = @_;

  my $mutect_directory = $cfg->{mutect_directory} // croak("Missing mutect_directory configuration");
  my $varscan_directory = $cfg->{varscan_directory} // croak("Missing varscan_directory configuration");
  my @mutect_commands = UHN::BuildCommands::scan_paths($cfg, \&import_mutect_file, $mutect_directory);
  my @varscan_commands = UHN::BuildCommands::scan_paths($cfg, \&import_varscan_file, $varscan_directory);
  my @commands = (@mutect_commands, @varscan_commands);

  $#$commands = -1;
  push @$commands, (@mutect_commands, @varscan_commands);
  return $commands;
}

sub execute_commands {
  my ($cfg, $commands) = @_;

  my $pm = new Parallel::ForkManager($cfg->{max_processes});
  foreach my $command (@$commands) {
    my @args = ($command->{script}, @{$command->{arguments}});
    $cfg->{LOGGER}->info("Processing file $command->{index}: $command->{description}");
    $cfg->{LOGGER}->info("Executing: " . join(" ", @args));

    my $pid = $pm->start and next;
    system(@args) == 0 or do {
      $cfg->{LOGGER}->error("Command failed: $?");
      croak($?);
    };
    $pm->finish;
  }
}

sub import_mutect_file {
  my ($cfg,  $base, $path) = @_;
  import_vcf_file($cfg, 'mutect', $base, $path);
}

sub import_varscan_file {
  my ($cfg,  $base, $path) = @_;
  import_vcf_file($cfg, 'varscan', $base, $path);
}

sub write_clinical_data {
  my ($cfg, $output, $commands, $cases) = @_;

  my @headers = (
    {name => 'PATIENT_ID', description => 'Patient Identifier', type => 'STRING', label => 'PATIENT', header => 'PATIENT_ID', count => 1},
    {name => 'SAMPLE_ID', description => 'Sample Identifier', type => 'STRING', label => 'SAMPLE', header => 'SAMPLE_ID', count => 1},
    {name => 'OS_MONTHS', description => 'Overall Survival', type => 'NUMBER', label => 'PATIENT', header => 'OS_MONTHS', count => 1},
    {name => 'OS_STATUS', description => 'Overall Status', type => 'STRING', label => 'PATIENT', header => 'OS_STATUS', count => 1},
    {name => 'CANCER_TYPE', description => 'Cancer Type', type => 'STRING', label => 'PATIENT', header => 'CANCER_TYPE', count => 1},
    {name => 'AGE_DIAGNOSIS', description => 'Age at Diagnosis', type => 'NUMBER', label => 'PATIENT', header => 'AGE_DIAGNOSIS', count => 1},
    {name => 'AGE_BIOPSY', description => 'Age at Biopsy', type => 'NUMBER', label => 'PATIENT', header => 'AGE_BIOPSY', count => 1},
    {name => 'SEX', description => 'Sex', type => 'STRING', label => 'PATIENT', header => 'SEX', count => 1},
    {name => 'YEAR_DIAGNOSIS', description => 'Year of Diagnosis', type => 'STRING', label => 'PATIENT', header => 'YEAR_DIAGNOSIS', count => 1},
    {name => 'PRIMARY_SITE', description => 'Cancer Type', type => 'STRING', label => 'PATIENT', header => 'PRIMARY_SITE', count => 1},
    {name => 'ONCOTREE_CODE', description => 'Cancer Type', type => 'STRING', label => 'PATIENT', header => 'ONCOTREE_CODE', count => 1},
  );

  my $output_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";
  $output_fh->print("#" . join("\t", map { $_->{name} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{description} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{type} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{label} } @headers) . "\n");
  $output_fh->print("#" . join("\t", map { $_->{count} } @headers) . "\n");

  my @header_names = map { $_->{header} } @headers;
  $output_fh->print(join("\t", @header_names). "\n");

  foreach my $command (@$commands) {
    my $patient = $command->{patient};
    my $case = $cases->{$patient} // croak("Can't find patient case data: $patient");
    my %record = ();
    @record{@header_names} = map { $case->{$_}; } @header_names;
    $record{PATIENT_ID} = $command->{patient};
    $record{SAMPLE_ID} = $command->{sample};
    my @values = map map { $record{$_}; } @header_names;
    $output_fh->print(join("\t", @values) . "\n");
  }
}

sub write_extended_mutations_data {
  my ($cfg, $output, @mafs) = @_;

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
  my ($cfg, $type, $base, $path) = @_;

  $cfg->{_vcf_count} //= 1;

  my ($tumour, $normal) = UHN::Samples::get_sample_identifiers($path);
  if (! $tumour || ! $normal) {
    $cfg->{LOGGER}->error("Can't extract tumour/normal sample identifiers from: $path");
    croak("Can't extract tumour/normal sample identifiers from: $path");
  }

  my $patient = $tumour;
  if ($patient =~ s{$cfg->{mapping}->{sample_pattern}}{$cfg->{mapping}->{patient_pattern}}ee) {
    ## Good to go
  } else {
    croak("Can't match sample pattern: " . $cfg->{mapping}->{sample_pattern});
  }

  ## Make a temporary file place, but we need to track this, because we
  ## are going to need this file...

  my $directory = "$cfg->{TEMP_DIRECTORY}";
  my ($temp_fh, $temp_filename) = tempfile( "${type}_XXXXXXXX", SUFFIX => '.maf', DIR => $directory);
  $temp_fh->close();
  unlink($temp_filename);

  my $command = {
    script => $cfg->{vcf2maf},
    output => $temp_filename,
    patient => $patient,
    sample => $tumour,
    type => $type,
    index => $cfg->{_vcf_count}++,
    description => "vcf2maf $path",
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
      '--output-maf', $temp_filename,
    ]
  };

  push @{$command->{arguments}}, '--no-vep-check-ref' if ($cfg->{no_vep_check_ref});

  return $command;
}

sub write_meta_file {
  my ($file, $data) =  @_;
  open(my $fh, ">", $file) || croak("Can't open file: $file: $!");
  foreach my $key (sort keys %$data) {
    print $fh "$key: $data->{$key}\n";
  }
  close($fh);
}

1;
