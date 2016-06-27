package UHN::Command;

use strict;
use warnings;

use Moose::Role;
use Digest::SHA;
use File::Copy;
use File::Spec;
use File::Path qw(make_path);
use File::Temp qw(tempfile);
use IO::Compress::Gzip qw(gzip $GzipError);
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);


has executable => (is => 'rw');
has script => (is => 'rw');
has output => (is => 'rw');
has output_type => (is => 'rw');
has patient => (is => 'rw');
has sample => (is => 'rw');
has index => (is => 'rw');
has description => (is => 'rw');
has options => (is => 'rw');
has arguments => (is => 'rw');
has executed => (is => 'rw');
has signature => (
  is => 'ro',
  default => sub { return Digest::SHA->new(256); }
);

sub add_signature_string {
  my ($self, $string) = @_;
  $self->signature()->add($string);
}

sub add_signature_file {
  my ($self, $file) = @_;
  $self->signature()->addfile($file);
}

sub get_signature {
  my ($self) = @_;
  return $self->signature()->hexdigest();
}

sub get_signature_path {
  $DB::single = 1;
  my ($self, $importer, $signature) = @_;
  my $cache_directory = $importer->cache_directory();
  my @fragments = ();
  push @fragments, substr($signature, 0, 2);
  push @fragments, $signature . ".data.gz";
  return File::Spec->catdir($cache_directory, @fragments);
}

sub execute {
  my ($self, $importer) = @_;
  my $cfg = $importer->cfg();

  my $signature = $self->get_signature($importer);
  my $cache = $self->get_signature_path($importer, $signature);
  if (-e $cache) {
    $self->executed(1);

    ## Copy and decompress from the cache
    my ($fh, $filename) = tempfile();
    gunzip $cache => $fh or die "gunzip failed: $GunzipError\n";
    close($fh);
    $self->output($filename);

    $importer->logger()->info("No need to execute (cache found): " . join(" ", @{$self->arguments()}));
    return;
  }

  $importer->logger()->info("Processing file ".$self->index().": ".$self->description());
  $importer->logger()->info("Executing: " . join(" ", @{$self->arguments()}));

  $self->executed(1);
  my @args = ($self->script(), @{$self->arguments()});
  if (defined $self->executable()) {
    unshift(@args, $self->executable());
  }

  system(@args) == 0 or do {
    $importer->logger()->error("Command failed ".$self->index().": status: $?");
    die($?);
  };

  my (undef, $directories) = File::Spec->splitpath($cache);
  make_path($directories);

  ## Compress the output to the cache
  $importer->logger()->info("Compressing command output into cache " . $self->output());
  gzip $self->output(), $cache or die "gzip failed: $GzipError\n";
  $importer->logger()->info("Command completed: ".$self->index().": ".$self->description());
}

1;
