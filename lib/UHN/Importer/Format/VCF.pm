package UHN::Importer::Format::VCF;

use strict;
use warnings;

use Carp;
use IO::File;
use File::Temp qw/ tempfile tempdir /;
use VCF;

use Moose;

with 'UHN::Format';

use Log::Log4perl;
my $log = Log::Log4perl->get_logger('UHN::Importer::Format::VCF');

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

sub BUILD {
  $log->info("Loading VCF format plugin");
}

sub handles_source {
  my ($self, $importer, $source) = @_;
  return ! defined($source->{format}) || $source->{format} eq 'vcf'
}

sub scan {
  my ($self, $importer, $pattern, $directory, $source_data, @args) = @_;
  $log->info("Scanning $directory");
  $self->scan_paths($importer, \&_import_file, $pattern, $directory, $source_data, @args)
}

sub _import_file {
  my ($self, $importer, $pattern, $path, $source, $options) = @_;
  my $cfg = $importer->cfg();

  VCF::validate($path);

  $cfg->{_vcf_count} //= 1;

  my $origin = $source->{origin} // die("Missing origin for source: $options->{source}");
  $options->{origin} = $origin;

  my ($tumour, $normal) = $self->get_sample_identifiers($importer, $options->{source}, $path);
  if (! $tumour || ! $normal) {
    $cfg->{LOGGER}->error("Can't extract tumour/normal sample identifiers from: $path");
    croak("Can't extract tumour/normal sample identifiers from: $path");
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

  ## Make a temporary file place, but we need to track this, because we
  ## are going to need this file...

  my $directory = "$cfg->{TEMP_DIRECTORY}";
  my ($temp_fh, $temp_filename) = tempfile( "import_vcf_XXXXXXXXX", SUFFIX => '.maf', DIR => $directory);
  $temp_fh->close();
  unlink($temp_filename);

  my $script_path = File::Spec->rel2abs($cfg->{vcf2maf}, $FindBin::Bin);

  my $command = UHN::Importer::Command::VCF->new();
  $command->executable($^X);
  $command->script($script_path);
  $command->output($temp_filename);
  $command->output_type('mutations');
  $command->patient($patient);
  $command->sample($tumour);
  $command->index($cfg->{_vcf_count}++);
  $command->description("vcf2maf for $patient $tumour $path");
  $command->options($options);
  my $arguments = [
    '--input-vcf', $path,
    '--tumor-id', $tumour,
    '--normal-id', $normal,
    '--vcf-tumor-id', $tumour,
    '--vcf-normal-id', $normal,
    '--vep-path', $cfg->{vep_path},
    '--vep-data', $cfg->{vep_data},
    '--vep-dir-plugins', $cfg->{vep_plugins},
    '--vep-forks', $cfg->{vep_forks},
    '--ref-fasta', $cfg->{ref_fasta},
    '--vep-buffer-size', $cfg->{vep_buffer_size},
    '--output-maf', $temp_filename,
    '--vep-extra-options', ($cfg->{vep_extra_options} // '')
  ];
  $command->arguments($arguments);

  ## Load up a signature so we can skip if we've seen the file before
  $command->add_signature_string('VCF');
  $command->add_signature_string($script_path);
  $command->add_signature_string($tumour);
  $command->add_signature_string($normal);
  $command->add_signature_file($path);

  push @{$command->arguments()}, '--no-vep-check-ref' if ($cfg->{no_vep_check_ref});
  push @{$command->arguments()}, '--vep-extra-options', $cfg->{vep_extra_options} if ($cfg->{vep_extra_options});

  return $command;
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
    if (/^#CHROM\s+/) {
      $header = $_;
      last;
    }
  };
  close($fh);

  my @values = ();
  my @result = ();
  if (defined $header) {
    my @fields = split(/\t/, $header);
    @values = @fields[9..$#fields];
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

sub finish {
  my ($self, $importer, $commands) = @_;
  my $cfg = $importer->cfg();

  my $mutations_data_file = File::Spec->catfile($cfg->{OUTPUT}, "data_mutations_extended.txt");

  if ($cfg->{overwrite} || ! -e $mutations_data_file) {
    if (! $cfg->{_dry_run}) {
      $self->write_extended_mutations_data($importer, $mutations_data_file, $commands);
    }
  }
  $self->write_mutations_meta_file($importer, $commands);
}

sub write_extended_mutations_data {
  my ($self, $importer, $output, $commands) = @_;
  my $cfg = $importer->cfg();

  die("No commands") if (! defined($commands));

  return if ($cfg->{_dry_run});

  ## Ensure we have the mapping
  $importer->get_gene_mapping();

  $log->info("Merging mutations MAF files into: $output");
  my @outputs = map { ($_->isa('UHN::Importer::Command::VCF')) ? ($_) : () } (@$commands);

  my $maf_fh = IO::File->new($output, ">") or croak "ERROR: Couldn't open output file: $output!\n";

  my $header1 = "#version 2.4\n";
  my $header2 = join("\t", @maf_header) . "\n";
  $maf_fh->print($header1 . $header2); # Print MAF header

  my $ensembl_table = $importer->ensembl_to_refseq_table();

  foreach my $output (@outputs) {
    my $maf = $output->output();
    $log->info("Reading generated mutations data: $maf");
    my $input_fh = IO::File->new($maf, "<") or carp "ERROR: Couldn't open input file: $maf!\n";
    while(<$input_fh>) {
      next if $_ eq $header1;
      next if $_ eq $header2;
      carp("Suspicious header: $_") if /^Hugo_Symbol/i;
      my ($symbol, $gene, $rest) = split(/\t/, $_, 3);
      next unless ($symbol && $gene);

      ## If we're seeing an Ensembl gene identifier, then we can map that using our
      ## friendly table.
      $DB::single = 1 if ($symbol eq 'BMP1');
      if ($gene =~ /^ENSG/ && exists($ensembl_table->{$gene})) {
        $_ = "$symbol\t$ensembl_table->{$gene}\t$rest";
      } elsif ($gene =~ /^ENSG/) {
        $log->warn("Skipping record for $symbol, $gene due to no accessible Entrez Gene identifier");
        next;
      }

      $maf_fh->print($_);
    }
    $input_fh->close();
  }
  $maf_fh->close();
}

sub write_mutations_meta_file {
  my ($self, $importer, $commands) = @_;
  my $cfg = $importer->cfg();

  my %meta = ();
  $meta{cancer_study_identifier} =         $cfg->{cancer_study}->{identifier};
  $meta{stable_id} =                       $meta{cancer_study_identifier}."_mutations";
  $meta{genetic_alteration_type} =         $cfg->{mutations}->{genetic_alteration_type};
  $meta{datatype} =                        $cfg->{mutations}->{datatype};
  $meta{show_profile_in_analysis_tab} =    $cfg->{mutations}->{show_profile_in_analysis_tab};
  $meta{profile_description} =             $cfg->{mutations}->{profile_description};
  $meta{profile_name} =                    $cfg->{mutations}->{profile_name};

  my $mutations_meta_file = File::Spec->catfile($cfg->{OUTPUT}, "meta_mutations_extended.txt");
  $importer->write_meta_file($mutations_meta_file, \%meta);
}

package UHN::Importer::Command::VCF;

use strict;
use warnings;

use Moose;

with 'UHN::Command';

__PACKAGE__->meta->make_immutable;

1;
