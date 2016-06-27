package UHN::Command;

use strict;
use warnings;

use Moose::Role;
use Digest::SHA;
use File::Copy;
use File::Spec;
use File::Path qw(make_path);

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
  push @fragments, $signature . ".data";
  return File::Spec->catdir($cache_directory, @fragments);
}

sub execute {
  my ($self, $importer) = @_;
  my $cfg = $importer->cfg();

  my $signature = $self->get_signature($importer);
  my $cache = $self->get_signature_path($importer, $signature);
  if (-e $cache) {
    $importer->logger()->info("No need to execute (cache found): " . join(" ", @{$self->arguments()}));
    $self->executed(1);
    $self->output($cache);
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
  copy($self->output(), $cache) or die "Copy failed: $!";
  $importer->logger()->info("Command completed: ".$self->index().": ".$self->description());
}

1;