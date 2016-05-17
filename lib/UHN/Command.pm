package UHN::Command;

use strict;
use warnings;

use Moose::Role;

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

sub execute {
  my ($self, $importer) = @_;
  my $cfg = $importer->cfg();
  $importer->logger()->info("Processing file ".$self->index().": ".$self->description());
  $importer->logger()->info("Executing: " . join(" ", @{$self->arguments()}));

  $self->executed(1);
  my @args = ($self->script(), @{$self->arguments()});

  system(@args) == 0 or do {
    $importer->logger()->error("Command failed ".$self->index().": status: $?");
    die($?);
  };
  $importer->logger()->info("Command completed: ".$self->index().": ".$self->description());
}

1;
