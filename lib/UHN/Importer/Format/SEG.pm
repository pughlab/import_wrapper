package UHN::Importer::Format::SEG;

use strict;
use warnings;

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
  my ($self, $importer, $pattern, $directory) = @_;
  $log->info("Scanning $directory");
  return ();
}

sub finish {
  my ($self, $importer, $commands) = @_;
}

1;
